package nats

import (
	"bytes"
	"context"
	"io"

	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/cloudevents/sdk-go/v2/binding/format"
	"github.com/hhk7734/equeue.go"
	"github.com/nats-io/nats.go"
)

func Open(url string, opts ...nats.Option) *Driver {
	n, err := nats.Connect(url, opts...)
	if err != nil {
		panic(err)
	}

	js, err := n.JetStream()
	if err != nil {
		panic(err)
	}

	return &Driver{JetStream: js}
}

var _ equeue.Driver = new(Driver)

type Driver struct {
	JetStream nats.JetStreamContext
}

func (n *Driver) Client() nats.JetStreamContext {
	return n.JetStream
}

func (n *Driver) Send(ctx context.Context, topic string, msg binding.Message) error {
	var err error
	defer msg.Finish(err)

	writer := new(bytes.Buffer)
	if err = WriteMsg(ctx, msg, writer); err != nil {
		return err
	}

	// TODO: custom subject from topic and Event
	_, err = n.JetStream.Publish(topic, writer.Bytes(), nats.ExpectStream(topic))
	return err
}

func (n *Driver) Consumer(topic string, subscriptionName string) (equeue.Consumer, error) {
	if _, err := n.JetStream.ConsumerInfo(topic, subscriptionName); err != nil {
		return nil, err
	}

	sub, err := n.JetStream.PullSubscribe("", subscriptionName, nats.BindStream(topic))
	if err != nil {
		return nil, err
	}

	ctx, cancelCtx := context.WithCancel(context.Background())

	return &natsConsumer{sub: sub, ctx: ctx, cancelCtx: cancelCtx}, nil
}

func (n *Driver) Close() error {
	return nil
}

func WriteMsg(ctx context.Context, m binding.Message, writer io.ReaderFrom, transformers ...binding.Transformer) error {
	structuredWriter := &natsMessageWriter{writer}

	_, err := binding.Write(
		ctx,
		m,
		structuredWriter,
		nil,
		transformers...,
	)
	return err
}

var _ binding.StructuredWriter = new(natsMessageWriter)

type natsMessageWriter struct {
	io.ReaderFrom
}

func (w *natsMessageWriter) SetStructuredEvent(_ context.Context, _ format.Format, event io.Reader) error {
	if _, err := w.ReadFrom(event); err != nil {
		return err
	}

	return nil
}
