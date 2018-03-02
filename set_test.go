package crudite

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/Shopify/sarama"
)

func TestSetAdd(t *testing.T) {
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
	s := tm.NewSet("bar")
	if err := s.Add([]byte("asdf")); err != nil {
		t.Fatalf("add should have succeeded: %v", err)
	}
	if !s.Contains([]byte("asdf")) {
		t.Fatalf("element missing")
	}
	es := s.Elements()
	if len(es) != 1 {
		t.Fatalf("bad elements length: %v", len(es))
	}
	if string(es[0]) != "asdf" {
		t.Fatalf("bad value for element: %v", string(es[0]))
	}
}

func TestSetElementsWithBackgroundUpdate(t *testing.T) {
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
	s := tm.NewSet("bar")
	if s.Name != "bar" {
		t.Fatalf("bad name: %v", s.Name)
	}
	if err := s.Add([]byte("zxcvb")); err != nil {
		t.Fatalf("add should have succeeded: %v", err)
	}
	op := operationMessage{
		ID:           "bar" + aSetIDSuffix,
		AbstractID:   "bar",
		AbstractType: LWWSet,
		Type:         aSet,
		Message:      opMsg,
		Element:      []byte("qwerty"),
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
	op.ID = "bar" + rSetIDSuffix
	op.Type = rSet
	op.Element = []byte("zxcvb")
	b, _ = json.Marshal(&op)
	k.coutput <- &sarama.ConsumerMessage{
		Key:       []byte(op.ID),
		Value:     b,
		Offset:    1,
		Topic:     ops.Topic,
		Partition: 0,
		Timestamp: time.Now().UTC().Add(1 * time.Second),
	}
	k.coutput <- &sarama.ConsumerMessage{}
	if !s.Contains([]byte("qwerty")) {
		t.Fatalf("element 'qwerty' expected")
	}
	if s.Contains([]byte("zxcvb")) {
		t.Fatalf("element 'zxcvb' should be absent")
	}
}
