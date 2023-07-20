package equeue

import (
	"context"

	cevents "github.com/cloudevents/sdk-go/v2"
)

type Driver interface {
	Publish(c context.Context, topic string, event *cevents.Event) error
	Consumer(topic string, subscriptionName string, maxAckPending int) (Consumer, error)
	Close() error
}
