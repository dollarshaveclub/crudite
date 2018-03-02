package crudite

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/Shopify/sarama"
)

func TestCounterIncrement(t *testing.T) {
	ops := Options{Topic: "omg", LogFunction: t.Logf}
	k := &fakeKafka{
		coutput: make(chan *sarama.ConsumerMessage),
		pinput:  make(chan *sarama.ProducerMessage, 1),
	}
	tm, err := newTypeManagerWithKafka(ops, k)
	if err != nil {
		t.Fatalf("error creating type manager: %v", err)
	}
	defer tm.Stop()
	c := tm.NewCounter("foo")
	if c.Name != "foo" {
		t.Fatalf("bad name: %v", c.Name)
	}
	if err := c.Increment(1); err != nil {
		t.Fatalf("should have succeeded: %v", err)
	}
	if c.Value() != 1 {
		t.Fatalf("bad value: %v", c.Value())
	}
	if len(k.pinput) != 1 {
		t.Fatalf("no op msg created")
	}
}

func TestCounterValueWithBackgroundUpdate(t *testing.T) {
	ops := Options{Topic: "omg", LogFunction: t.Logf}
	k := &fakeKafka{
		coutput: make(chan *sarama.ConsumerMessage),
		pinput:  make(chan *sarama.ProducerMessage, 1),
	}
	tm, err := newTypeManagerWithKafka(ops, k)
	if err != nil {
		t.Fatalf("error creating type manager: %v", err)
	}
	defer tm.Stop()
	go tm.listener()
	c := tm.NewCounter("foo")
	if c.Name != "foo" {
		t.Fatalf("bad name: %v", c.Name)
	}
	op := operationMessage{
		ID:           "foo" + pCounterIDSuffix,
		AbstractID:   "foo",
		AbstractType: PNCounter,
		Type:         pCounter,
		Message:      opMsg,
		Delta:        1,
	}
	b, _ := json.Marshal(&op)
	k.coutput <- &sarama.ConsumerMessage{
		Key:       []byte(op.ID),
		Value:     b,
		Offset:    1,
		Topic:     ops.Topic,
		Partition: 0,
		Timestamp: time.Now().UTC(),
	}
	k.coutput <- &sarama.ConsumerMessage{}
	if c.Value() != 1 {
		t.Fatalf("bad value: %v", c.Value())
	}
}
