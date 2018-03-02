package crudite

import (
	"fmt"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/google/uuid"
	"github.com/pkg/errors"
)

// kakfa is a wrapper around Kafka I/O to facilitate testing
type kafka interface {
	ConsumerMessages() <-chan *sarama.ConsumerMessage
	ProducerInput() chan<- *sarama.ProducerMessage
	MarkOffset(*sarama.ConsumerMessage, string)
	HighWaterMarks() map[string]map[int32]int64
	NewConsumer(ops Options, initialOffset int64) (kafka, error)
	Close() error
}

var _ kafka = &realKafka{}
var _ kafka = &fakeKafka{}

func newRealKafka(ops Options, rid uuid.UUID) (kafka, error) {
	scfg := &sarama.Config{}
	scfg.Net.MaxOpenRequests = int(ops.OutputQueueSize)
	c, err := sarama.NewClient(ops.Brokers, scfg)
	if err != nil {
		return nil, errors.Wrap(err, "error getting Kafka client")
	}
	ap, err := sarama.NewAsyncProducerFromClient(c)
	if err != nil {
		return nil, errors.Wrap(err, "error getting async Kafka producer")
	}
	cc := &cluster.Client{Client: c}
	cr, err := cluster.NewConsumerFromClient(cc, rid.String(), []string{ops.Topic})
	if err != nil {
		return nil, errors.Wrap(err, "error getting Kafka consumer")
	}
	return &realKafka{
		ap: ap,
		cc: cr,
	}, nil
}

type realKafka struct {
	ap sarama.AsyncProducer
	cc *cluster.Consumer
}

// NewConsumer`returns an instance of kafka with a new consumer configured according to ops and initialOffset. There is no producer.
func (rk *realKafka) NewConsumer(ops Options, initialOffset int64) (kafka, error) {
	rid, err := uuid.NewRandom()
	if err != nil {
		return nil, errors.Wrap(err, "error getting random ID")
	}
	cfg := cluster.Config{}
	cfg.Consumer.Offsets.Initial = initialOffset
	c, err := cluster.NewConsumer(ops.Brokers, rid.String(), []string{ops.Topic}, &cfg)
	if err != nil {
		return nil, errors.Wrap(err, "error creating Kafka consumer")
	}
	return &realKafka{
		cc: c,
	}, nil
}

// ConsumerMessages returns the channel containing incoming Kafka messages
func (rk *realKafka) ConsumerMessages() <-chan *sarama.ConsumerMessage {
	return rk.cc.Messages()
}

// ProducerInput returns the input channel for the Kafka producer
func (rk *realKafka) ProducerInput() chan<- *sarama.ProducerMessage {
	return rk.ap.Input()
}

// MarkOffset marks m as processed and includes metadata d
func (rk *realKafka) MarkOffset(m *sarama.ConsumerMessage, d string) {
	rk.cc.MarkOffset(m, d)
}

// HighWaterMarks returns the high water mark for each partition within each topic
func (rk *realKafka) HighWaterMarks() map[string]map[int32]int64 {
	return rk.cc.HighWaterMarks()
}

// Close cleanly stops the producer and the consumer
func (rk *realKafka) Close() error {
	err := rk.ap.Close()
	err2 := rk.cc.Close()
	if err != nil || err2 != nil {
		return fmt.Errorf("error closing: producer: %v; consumer: %v", err, err2)
	}
	return nil
}

type fakeKafka struct {
	hwm     map[string]map[int32]int64
	coutput chan *sarama.ConsumerMessage
	pinput  chan *sarama.ProducerMessage
}

// ConsumerMessages returns the fake consumer output channel
func (fk *fakeKafka) ConsumerMessages() <-chan *sarama.ConsumerMessage {
	return fk.coutput
}

// ProducerInput returns the fake producer input channel
func (fk *fakeKafka) ProducerInput() chan<- *sarama.ProducerMessage {
	return fk.pinput
}

// MarkOffset does nothing
func (fk *fakeKafka) MarkOffset(_ *sarama.ConsumerMessage, _ string) {
}

func (fk *fakeKafka) HighWaterMarks() map[string]map[int32]int64 {
	return fk.hwm
}

// NewConsumer returns the same consumer
func (fk *fakeKafka) NewConsumer(ops Options, initialOffset int64) (kafka, error) {
	return fk, nil
}

// Close closes the channels
func (fk *fakeKafka) Close() error {
	close(fk.coutput)
	close(fk.pinput)
	return nil
}
