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

		msg := newNatsReceiveMessage(ctx, nMsg)

		return msg, nil
	}
}

func (n *natsConsumer) Stop() error {
	n.cancelCtx()
	return nil
}

var _ binding.Message = new(natsReceiveMessage)
var _ binding.MessageContext = new(natsReceiveMessage)

type natsReceiveMessage struct {
	msg      *nats.Msg
	format   format.Format
	encoding binding.Encoding

	ctx context.Context
}

func newNatsReceiveMessage(ctx context.Context, msg *nats.Msg) *natsReceiveMessage {
	f := equeue.ForcedReceivedMessageFormat(ctx)
	if f == nil {
		f = format.JSON
	}
	return &natsReceiveMessage{
		msg:    msg,
		format: f,
		ctx:    ctx,
	}
}

func (n *natsReceiveMessage) Context() context.Context {
	return n.ctx
}

func (n *natsReceiveMessage) ReadEncoding() binding.Encoding {
	return n.encoding
}

func (n *natsReceiveMessage) ReadStructured(ctx context.Context, builder binding.StructuredWriter) error {
	return builder.SetStructuredEvent(ctx, n.format, bytes.NewReader(n.msg.Data))
}

func (n *natsReceiveMessage) ReadBinary(ctx context.Context, builder binding.BinaryWriter) error {
	return binding.ErrNotBinary
}

func (n *natsReceiveMessage) Finish(err error) error {
	if protocol.IsACK(err) {
		return n.msg.Ack()
	} else {
		return n.msg.Nak()
	}
}
