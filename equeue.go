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
		tree: make(map[string]map[string]HandlersChain),
	}
	engine.RouterGroup.engine = engine
	engine.pool.New = func() interface{} {
		return engine.newContext()
	}
	return engine
}

type Engine struct {
	RouterGroup

	inShutdown atomic.Bool

	pool sync.Pool
	tree map[string]map[string]HandlersChain
}

var _ Router = new(Engine)

func (e *Engine) addRoute(topic string, subscriptionName string, handlers HandlersChain) {
	if topic == "" {
		panic("topic can not be empty")
	}

	if subscriptionName == "" {
		panic("subscription name can not be empty")
	}

	if len(handlers) == 0 {
		panic("there must be at least one handler")
	}

	if _, ok := e.tree[topic]; !ok {
		e.tree[topic] = make(map[string]HandlersChain)
	}

	if _, ok := e.tree[topic][subscriptionName]; ok {
		panic("duplicated subscription")
	}

	e.tree[topic][subscriptionName] = handlers
}

func (e *Engine) newContext() *Context {
	return &Context{}
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
