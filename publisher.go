package equeue

import (
	"context"

	"github.com/cloudevents/sdk-go/v2/event"
)

type Publisher interface {
	Publish(c context.Context, topic string, event *event.Event) error
}
