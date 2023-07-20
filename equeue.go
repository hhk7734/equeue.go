package equeue

import (
	"context"
	"sync/atomic"
)

type HandlerFunc func(*Context)

type HandlersChain []HandlerFunc

type Engine struct {
	RouterGroup

	subscriptions []subscription
	inShutdown    atomic.Bool
}

var _ Router = new(Engine)

func (engine *Engine) addRoute(topic string, subscriptionName string, handlers HandlersChain) {
	if topic == "" {
		panic("topic can not be empty")
	}

	for _, s := range engine.subscriptions {
		if s.Topic == topic && s.SubscriptionName == subscriptionName {
			panic("duplicated subscription")
		}
	}

	if len(handlers) == 0 {
		panic("there must be at least one handler")
	}

	engine.subscriptions = append(engine.subscriptions, subscription{
		Topic:            topic,
		SubscriptionName: subscriptionName,
		Handlers:         handlers,
	})
}

type subscription struct {
	Topic            string
	SubscriptionName string
	Handlers         HandlersChain
}

func (e *Engine) Subscribe() error {
	if e.shuttingDown() {
		return ErrServerClosed
	}
	return nil
}

func (e *Engine) Shutdown(ctx context.Context) error {
	e.inShutdown.Store(true)
	return nil
}

func (e *Engine) shuttingDown() bool {
	return e.inShutdown.Load()
}
