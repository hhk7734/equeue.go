package equeue

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
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

func (e *Engine) Run() error {
	if e.shuttingDown() {
		return ErrServerClosed
	}
	defer e.Close()

	return nil
}

func (e *Engine) Close() {
	e.inShutdown.Store(true)
}

const shutdownPollIntervalMax = 500 * time.Millisecond

func (e *Engine) Shutdown(ctx context.Context) error {
	e.inShutdown.Store(true)

	// TODO: Close consumers

	pollIntervalBase := time.Millisecond
	nextPollInterval := func() time.Duration {
		interval := pollIntervalBase + time.Duration(rand.Intn(int(pollIntervalBase/10)))
		pollIntervalBase *= 2
		if pollIntervalBase > shutdownPollIntervalMax {
			pollIntervalBase = shutdownPollIntervalMax
		}
		return interval
	}

	timer := time.NewTimer(nextPollInterval())
	defer timer.Stop()
	for {
		// TODO: check if there are any active msg
		if false {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			timer.Reset(nextPollInterval())
		}
	}
}

func (e *Engine) shuttingDown() bool {
	return e.inShutdown.Load()
}
