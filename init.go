package crudite

import (
	"context"
	"encoding/json"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
)

// ErrInitAborted is returned when Init is aborted via context
var ErrInitAborted = errors.New("init was aborted")

// logOp wraps a deserialized operationMessage with kafka metadata
type logOp struct {
	offset    int64
	timestamp time.Time
	op        operationMessage
}

// Init synchronously reads through the event logs and populates TypeManager with any existing data structures and values. If the event log is sizeable, this could block for a substantial period of time.
func (tm *TypeManager) Init(ctx context.Context) error {
	// get all messages to head
	// get latest snapshot for each crdt
	// restore latest snapshot
	// replay log from offset of snapshot
	k, err := tm.k.NewConsumer(tm.ops, sarama.OffsetOldest)
	if err != nil {
		return errors.Wrap(err, "error getting new Kafka consumer")
	}

	hwm := k.HighWaterMarks()[tm.ops.Topic]
	if len(hwm) == 0 {
		return errors.New("empty high water marks")
	}

	sm := map[string]operationMessage{}
	opm := map[string][]logOp{}
	var partdone int

	for m := range k.ConsumerMessages() {
		select {
		case <-ctx.Done():
			return ErrInitAborted
		default:
		}
		op := operationMessage{}
		if err := json.Unmarshal(m.Value, &op); err != nil {
			tm.lf("error unmarshaling message: %v", err)
			continue
		}
		switch op.Message {
		case snapshotMsg:
			sm[op.ID] = op
		case opMsg:
			lo := logOp{op: op, offset: m.Offset, timestamp: m.Timestamp}
			if ops, ok := opm[op.ID]; ok {
				ops = append(ops, lo)
				opm[op.ID] = ops
			} else {
				opm[op.ID] = []logOp{lo}
			}
		}
		if m.Offset >= hwm[m.Partition] {
			partdone++
		}
		if partdone == len(hwm) {
			break // all partitions have hit the high water mark
		}
	}

	cm := map[string]*crdt{}
	am := map[string]*abstractCRDT{}

	for id, ssop := range sm {
		cm[id] = &crdt{
			id:     id,
			dtype:  ssop.Type,
			cvalue: uint64(ssop.Snapshot.CValue),
			sval:   ssop.Snapshot.SElements,
			offset: ssop.Snapshot.Offset,
		}
		if _, ok := am[ssop.AbstractID]; !ok {
			am[ssop.AbstractID] = &abstractCRDT{
				id:         ssop.AbstractID,
				dtype:      ssop.AbstractType,
				components: map[DataType]*crdt{},
			}
		}
		am[ssop.AbstractID].components[ssop.Type] = cm[id]
		for _, op := range opm[id] {
			if op.offset > ssop.Snapshot.Offset {
				cm[id].op(&op.op, op.offset, op.timestamp)
			}
		}
	}

	tm.contains.Lock()
	tm.contains.values = cm
	tm.contains.abstracts = am
	tm.contains.Unlock()

	return nil
}
