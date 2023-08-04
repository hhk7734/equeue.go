package nats

import (
	"bytes"
	"context"
	"errors"

	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/cloudevents/sdk-go/v2/binding/format"
	"github.com/cloudevents/sdk-go/v2/protocol"
	"github.com/hhk7734/equeue.go"
	"github.com/nats-io/nats.go"
)

var _ equeue.Consumer = new(natsConsumer)

type natsConsumer struct {
	sub       *nats.Subscription
	ctx       context.Context
	cancelCtx context.CancelFunc
}

func (n *natsConsumer) Receive(ctx context.Context) (binding.Message, error) {
	for {
		select {
		case <-n.ctx.Done():
			return nil, equeue.ErrConsumerStopped
		default:
		}

		nMsgs, err := n.sub.Fetch(1, nats.Context(n.ctx))
		switch {
		case errors.Is(err, nats.ErrTimeout):
			continue
		case errors.Is(err, context.DeadlineExceeded):
			continue
		case errors.Is(err, context.Canceled):
			return nil, equeue.ErrConsumerStopped
		case err != nil:
			return nil, err
		}
		nMsg := nMsgs[0]

		msg := &natsMessage{msg: nMsg}

		return msg, nil
	}
}

func (n *natsConsumer) Stop() error {
	n.cancelCtx()
	return nil
}

var _ binding.Message = new(natsMessage)

type natsMessage struct {
	msg      *nats.Msg
	encoding binding.Encoding
}

func (n *natsMessage) ReadEncoding() binding.Encoding {
	return n.encoding
}

func (n *natsMessage) ReadStructured(ctx context.Context, builder binding.StructuredWriter) error {
	return builder.SetStructuredEvent(ctx, format.JSON, bytes.NewReader(n.msg.Data))
}

func (n *natsMessage) ReadBinary(ctx context.Context, builder binding.BinaryWriter) error {
	return binding.ErrNotBinary
}

func (n *natsMessage) Finish(err error) error {
	if protocol.IsACK(err) {
		return n.msg.Ack()
	} else {
		return n.msg.Nak()
	}
}
