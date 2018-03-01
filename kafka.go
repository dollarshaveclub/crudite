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

// Close does nothing
func (fk *fakeKafka) Close() error {
	return nil
}
