package crudite

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"github.com/Shopify/sarama"
)

var basetime = time.Now().UTC()
var ops = []operationMessage{
	operationMessage{
		ID:           "fooc" + pCounterIDSuffix,
		AbstractID:   "fooc",
		AbstractType: PNCounter,
		Type:         pCounter,
		Message:      snapshotMsg,
		Snapshot: snapshot{
			Offset: 0,
			CValue: 10,
		},
	},
	operationMessage{
		ID:           "fooc" + nCounterIDSuffix,
		AbstractID:   "fooc",
		AbstractType: PNCounter,
		Type:         nCounter,
		Message:      snapshotMsg,
		Snapshot: snapshot{
			Offset: 0,
			CValue: 0,
		},
	},
	operationMessage{
		ID:           "bars" + aSetIDSuffix,
		AbstractID:   "bars",
		AbstractType: LWWSet,
		Type:         aSet,
		Message:      snapshotMsg,
		Snapshot: snapshot{
			Offset: 0,
			SElements: setValue{
				hex.EncodeToString([]byte("asdf1234")):   basetime,
				hex.EncodeToString([]byte("qwerty5678")): basetime,
			},
		},
	},
	operationMessage{
		ID:           "bars" + rSetIDSuffix,
		AbstractID:   "bars",
		AbstractType: LWWSet,
		Type:         rSet,
		Message:      snapshotMsg,
		Snapshot: snapshot{
			Offset:    0,
			SElements: setValue{},
		},
	},
	operationMessage{
		ID:           "fooc" + pCounterIDSuffix,
		AbstractID:   "fooc",
		AbstractType: PNCounter,
		Type:         pCounter,
		Delta:        1,
		Message:      opMsg,
	},
	operationMessage{
		ID:           "bars" + aSetIDSuffix,
		AbstractID:   "bars",
		AbstractType: LWWSet,
		Type:         aSet,
		Element:      []byte("zxcvb0000"),
		Message:      opMsg,
	},
	operationMessage{
		ID:           "fooc" + nCounterIDSuffix,
		AbstractID:   "fooc",
		AbstractType: PNCounter,
		Type:         nCounter,
		Delta:        2,
		Message:      opMsg,
	},
	operationMessage{
		ID:           "bars" + rSetIDSuffix,
		AbstractID:   "bars",
		AbstractType: LWWSet,
		Type:         rSet,
		Element:      []byte("zxcvb0000"),
		Message:      opMsg,
	},
}

func TestInit(t *testing.T) {
	opts := Options{Topic: "omg", LogFunction: t.Logf}
	k := &fakeKafka{
		coutput: make(chan *sarama.ConsumerMessage, len(ops)),
		//pinput:  make(chan *sarama.ProducerMessage),
		hwm: map[string]map[int32]int64{
			opts.Topic: map[int32]int64{
				0: int64(len(ops) - 1),
			},
		},
	}
	for i, op := range ops {
		b, _ := json.Marshal(&op)
		k.coutput <- &sarama.ConsumerMessage{
			Key:       []byte(op.ID),
			Value:     b,
			Offset:    int64(i),
			Topic:     opts.Topic,
			Partition: 0,
			Timestamp: basetime.Add(time.Duration(i) * time.Minute),
		}
	}
	tm, err := newTypeManagerWithKafka(opts, k)
	if err != nil {
		t.Fatalf("newTypeManager should have succeeded: %v", err)
	}
	err = tm.Init(context.Background())
	if err != nil {
		t.Fatalf("init should have succeeded: %v", err)
	}
	cnts := tm.Contains()
	for _, n := range []string{"fooc", "bars"} {
		if _, ok := cnts[n]; !ok {
			t.Fatalf("missing data structure: %v", n)
		}
	}
	if cnts["fooc"].Type != PNCounter {
		t.Fatalf("bad type for fooc: %v", cnts["fooc"].Type.String())
	}
	if cnts["bars"].Type != LWWSet {
		t.Fatalf("bad type for bars: %v", cnts["bars"].Type.String())
	}
	fc, err := tm.GetCounter("fooc")
	if err != nil {
		t.Fatalf("expected counter to exist: %v", err)
	}
	if fc.Value() != 9 {
		t.Fatalf("unexpected counter value: %v", fc.Value())
	}
	bs, err := tm.GetSet("bars")
	if err != nil {
		t.Fatalf("expected set to exist: %v", err)
	}
	ec := len(bs.Elements())
	if ec != 2 {
		t.Fatalf("bad number of set elements: %v", ec)
	}
	if !bs.Contains([]byte("asdf1234")) {
		t.Fatalf("expected set to contain asdf1234")
	}
	if !bs.Contains([]byte("qwerty5678")) {
		t.Fatalf("expected set to contain qwerty5678")
	}
}
