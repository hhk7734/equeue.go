package equeue

import (
	"context"

	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/cloudevents/sdk-go/v2/binding/format"
)

type Driver interface {
	// Send sends a message to the stream associated with the topic. msg.Finish() is called when
	// sending is done.
	Send(c context.Context, topic string, msg binding.Message) error
	Consumer(topic string, subscriptionName string) (Consumer, error)
	Close() error
}

type receivedMessageFormatKey struct{}

func withForcedReceivedMessageFormat(ctx context.Context, f format.Format) context.Context {
	return context.WithValue(ctx, receivedMessageFormatKey{}, f)
}

// ForcedReceivedMessageFormat returns format to convert the received message to event.Event. Do not
// use this function except in Driver implementations.
func ForcedReceivedMessageFormat(ctx context.Context) format.Format {
	f := ctx.Value(receivedMessageFormatKey{})
	if f == nil {
		return nil
	}
	return f.(format.Format)
}

type Consumer interface {
	// Receive blocks until a message is received or an error occurs. If Consumer is stopped,
	// ErrConsumerStoped is returned. The caller is responsible for calling `Finish()` on the
	// returned message.
	Receive(ctx context.Context) (binding.Message, error)
	Stop() error
}
