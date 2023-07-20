package equeue

import (
	"context"
	"sync"
	"sync/atomic"
)

type HandlerFunc func(*Context)

type HandlersChain []HandlerFunc

func New() *Engine {
	engine := &Engine{
		RouterGroup: RouterGroup{
			root: true,
		},
	}
	engine.RouterGroup.engine = engine
	engine.pool.New = func() interface{} {
		return engine.newContext()
	}
	return engine
}

type Engine struct {
	RouterGroup

	subscriptions []subscription
	inShutdown    atomic.Bool

	pool sync.Pool
}

var _ Router = new(Engine)

func (e *Engine) addRoute(topic string, subscriptionName string, handlers HandlersChain) {
	if topic == "" {
		panic("topic can not be empty")
	}

	for _, s := range e.subscriptions {
		if s.Topic == topic && s.SubscriptionName == subscriptionName {
			panic("duplicated subscription")
		}
	}

	if len(handlers) == 0 {
		panic("there must be at least one handler")
	}

	e.subscriptions = append(e.subscriptions, subscription{
		Topic:            topic,
		SubscriptionName: subscriptionName,
		Handlers:         handlers,
	})
}

func (e *Engine) newContext() *Context {
	return &Context{}
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
