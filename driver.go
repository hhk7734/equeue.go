package equeue

import (
	"context"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

type Driver interface {
	Publish(c context.Context, topic string, event cloudevents.Event) error
	Consumer(topic string, subscriptionName string) (Consumer, error)
	Close() error
}

type Consumer interface {
	// Receive blocks until a message is received or an error occurs. If Consumer is stopped,
	// ErrConsumerStoped is returned.
	Receive() (Message, error)
	Stop() error
}

type Message interface {
	Event() *cloudevents.Event
	Ack()
	Nack()
}
