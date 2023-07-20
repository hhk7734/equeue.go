package equeue

import "github.com/cloudevents/sdk-go/v2/event"

type Consumer interface {
	// Receive blocks until a message is received or an error occurs. If Consumer is stopped,
	// ErrConsumerStoped is returned.
	Receive() (Message, error)
	Stop() error
}

type Message interface {
	Event() *event.Event
}
