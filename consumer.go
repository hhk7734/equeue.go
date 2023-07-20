package equeue

import "github.com/cloudevents/sdk-go/v2/event"

type Consumer interface {
	Receive() (Message, error)
	Stop() error
}

type Message interface {
	Event() *event.Event
}

