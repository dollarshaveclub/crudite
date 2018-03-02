package crudite

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/google/uuid"
	"github.com/pkg/errors"
)

//go:generate stringer -type=DataType

// DataType desribes the type of CRDT
type DataType int

// Supported abstract CRDT types
const (
	PNCounter DataType = iota
	LWWSet
	// Internal primitive CRDT types
	pCounter
	nCounter
	aSet
	rSet
)

type msgType int

const (
	opMsg msgType = iota
	snapshotMsg
)

type snapshot struct {
	CValue    uint     `json:"cvalue"`
	SElements setValue `json:"selements"`
	Offset    int64    `json:"offset"`
}

// Kafka message
type operationMessage struct {
	ID           string   `json:"id"` // Name of the data structure
	AbstractID   string   `json:"abstract_id"`
	AbstractType DataType `json:"abstract_type"`
	ManagerID    string   `json:"manager_id"` // ID of TypeManager instance (to avoid processing own messages)
	Type         DataType `json:"data_type"`  // internal CRDT type
	Delta        uint     `json:"delta"`      // counter types
	Element      []byte   `json:"element"`    // set types
	Message      msgType  `json:"msg_type"`   // is this an op or a snapshot
	Snapshot     snapshot `json:"snapshot"`   // if snapshot, the current state
}

type setValue map[string]time.Time

func (sv setValue) add(elem []byte, ts time.Time) {
	sv[hex.EncodeToString(elem)] = ts
}

func (sv setValue) contains(elem []byte) (time.Time, bool) {
	ts, ok := sv[hex.EncodeToString(elem)]
	return ts, ok
}

type crdt struct {
	sync.RWMutex
	id     string
	dtype  DataType
	offset int64
	cvalue uint64
	sval   setValue
}

func (cr *crdt) op(msg *operationMessage, offset int64, ts time.Time) error {
	if msg.Type != cr.dtype {
		return fmt.Errorf("mismatched data types: %v (expected %v)", msg.Type.String(), cr.dtype.String())
	}
	if offset <= cr.offset {
		return fmt.Errorf("offset less than what has already been applied")
	}
	switch cr.dtype {
	case pCounter:
		fallthrough
	case nCounter:
		cr.cvalue += uint64(msg.Delta)
	case aSet:
		fallthrough
	case rSet:
		cr.sval.add(msg.Element, ts)
	default:
		return fmt.Errorf("unknown data type: %v", msg.Type.String())
	}
	cr.offset = offset
	return nil
}

// High level CRDTs are composed of several underlying CRDTs
type abstractCRDT struct {
	id         string
	dtype      DataType
	components map[DataType]*crdt
}

type lockingCRDTs struct {
	sync.RWMutex
	values    map[string]*crdt
	abstracts map[string]*abstractCRDT
}

// LogFunc is a function that logs a formatted string somewhere
type LogFunc func(string, ...interface{})

// TypeManager is an object that manages CRDT types
type TypeManager struct {
	ops      Options
	id       string
	k        kafka
	contains *lockingCRDTs
	stckr    *time.Ticker
	lf       LogFunc
}

// Options is the configuration for a TypeManager
type Options struct {
	Brokers            []string
	Topic              string
	OutputQueueSize    uint
	SnapshotInterval   time.Duration
	LogFunction        LogFunc
	FailOnBlockingSend bool // If output queue is full, fail instead of blocking on send
}

// ErrSendWouldHaveBlocked is an error indicating that the output queue was full
var ErrSendWouldHaveBlocked = errors.New("write to output channel would have blocked")

// Options defaults
const (
	DefaultSnapshotInterval = 10 * time.Minute
	DefaultOutputQueueSize  = 100 // number of outgoing log messages to queue before sending blocks
)

func (ops *Options) setdefaults() {
	if ops.SnapshotInterval == 0 {
		ops.SnapshotInterval = DefaultSnapshotInterval
	}
	if ops.OutputQueueSize == 0 {
		ops.OutputQueueSize = DefaultOutputQueueSize
	}
	if ops.LogFunction == nil {
		ops.LogFunction = func(string, ...interface{}) {}
	}
}

// NewTypeManager returns a TypeManager using the options provided
func NewTypeManager(ops Options) (*TypeManager, error) {
	ops.setdefaults()
	rid, err := uuid.NewRandom()
	if err != nil {
		return nil, errors.Wrap(err, "error getting random ID")
	}
	k, err := newRealKafka(ops, rid)
	if err != nil {
		return nil, errors.Wrap(err, "error getting Kafka clients")
	}
	tm := &TypeManager{
		ops: ops,
		k:   k,
		contains: &lockingCRDTs{
			values:    make(map[string]*crdt),
			abstracts: make(map[string]*abstractCRDT),
		},
		id: rid.String(),
		lf: ops.LogFunction,
	}
	go tm.listener()
	tm.snapshots()
	return tm, nil
}

// newTypeManagerWithKafka returns a TypeManager using the options provided and the provided kafka for testing
func newTypeManagerWithKafka(ops Options, k kafka) (*TypeManager, error) {
	ops.setdefaults()
	rid, err := uuid.NewRandom()
	if err != nil {
		return nil, errors.Wrap(err, "error getting random ID")
	}
	tm := &TypeManager{
		ops: ops,
		k:   k,
		contains: &lockingCRDTs{
			values:    make(map[string]*crdt),
			abstracts: make(map[string]*abstractCRDT),
		},
		id: rid.String(),
		lf: ops.LogFunction,
	}
	// listener and snapshots must be manually started
	return tm, nil
}

// DataStructure contains information about an extant data stucture
type DataStructure struct {
	Name string
	Type DataType
}

// Contains returns all known data structures as a map of name to DataStructure
func (tm *TypeManager) Contains() map[string]DataStructure {
	tm.contains.RLock()
	defer tm.contains.RUnlock()

	output := map[string]DataStructure{}

	for _, ab := range tm.contains.abstracts {
		ds := DataStructure{
			Name: ab.id,
			Type: ab.dtype,
		}
		output[ab.id] = ds
	}

	return output
}

func getsnapshot(v *crdt) (snapshot, error) {
	s := snapshot{Offset: v.offset}
	switch v.dtype {
	case pCounter:
		fallthrough
	case nCounter:
		s.CValue = uint(v.cvalue)
	case aSet:
		fallthrough
	case rSet:
		s.SElements = v.sval
	}
	return s, nil
}

func (tm *TypeManager) publishSnapshots() error {
	tm.contains.RLock()
	defer tm.contains.RUnlock()
	for _, v := range tm.contains.values {
		ss, err := getsnapshot(v)
		if err != nil {
			return errors.Wrap(err, "error getting snapshot")
		}
		msg := operationMessage{
			ID:        v.id,
			ManagerID: tm.id,
			Type:      v.dtype,
			Message:   snapshotMsg,
			Snapshot:  ss,
		}
		b, err := json.Marshal(&msg)
		if err != nil {
			return errors.Wrap(err, "error marshaling operationMessage")
		}
		tm.k.ProducerInput() <- &sarama.ProducerMessage{
			Topic: tm.ops.Topic,
			Key:   sarama.ByteEncoder(v.id),
			Value: sarama.ByteEncoder(b),
		}
	}
	return nil
}

func (tm *TypeManager) snapshots() {
	tm.stckr = time.NewTicker(tm.ops.SnapshotInterval)
	go func() {
		for _ = range tm.stckr.C {
			if err := tm.publishSnapshots(); err != nil {
				tm.lf("error publishing snapshots: %v", err)
			}
		}
	}()
}

func (tm *TypeManager) listener() {
	for m := range tm.k.ConsumerMessages() {
		tm.contains.RLock()
		if val, ok := tm.contains.values[string(m.Key)]; ok {
			opm := operationMessage{}
			if err := json.Unmarshal(m.Value, &opm); err != nil {
				tm.lf("error unmarshalling log message: %v", err)
				continue
			}
			// we only apply the message if it's an op (not a snapshot) and if it was published by a different node/manager
			if opm.Message == opMsg && opm.ManagerID != tm.id {
				val.Lock()
				if err := val.op(&opm, m.Offset, m.Timestamp); err != nil {
					tm.lf("error performing op: %v", err)
				}
				val.Unlock()
				tm.k.MarkOffset(m, "processed for "+opm.ID)
			}
		}
		tm.contains.RUnlock()
	}
}

func (tm *TypeManager) sendop(op *operationMessage) error {
	b, err := json.Marshal(&op)
	if err != nil {
		return errors.Wrap(err, "error marshaling op message")
	}
	pm := &sarama.ProducerMessage{
		Topic: tm.ops.Topic,
		Key:   sarama.ByteEncoder(op.ID),
		Value: sarama.ByteEncoder(b),
	}
	if tm.ops.FailOnBlockingSend {
		select {
		case tm.k.ProducerInput() <- pm:
			return nil
		default:
			return ErrSendWouldHaveBlocked
		}
	}
	tm.k.ProducerInput() <- pm // block
	return nil
}

// Stop shuts down the TypeManager
func (tm *TypeManager) Stop() {
	if tm.stckr != nil {
		tm.stckr.Stop()
	}
	if err := tm.k.Close(); err != nil {
		tm.lf("error closing kafka: %v", err)
	}
}
