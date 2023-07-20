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
		tree: make(subsTree),
	}
	engine.RouterGroup.engine = engine
	engine.pool.New = func() interface{} {
		return engine.newContext()
	}
	return engine
}

type subscription struct {
	topic            string
	subscriptionName string
	maxWorker        int
	handlers         HandlersChain
}

type subsTree map[string]map[string]subscription

type Engine struct {
	RouterGroup

	driver Driver

	inShutdown atomic.Bool

	pool sync.Pool
	tree subsTree

	mu            sync.Mutex
	consumers     map[*Consumer]struct{}
	consumerGroup sync.WaitGroup
}

var _ Router = new(Engine)

func (e *Engine) addRoute(topic string, subscriptionName string, maxWorker int, handlers HandlersChain) {
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
		e.tree[topic] = make(map[string]subscription)
	}

	if _, ok := e.tree[topic][subscriptionName]; ok {
		panic("duplicated subscription")
	}

	e.tree[topic][subscriptionName] = subscription{
		topic:            topic,
		subscriptionName: subscriptionName,
		maxWorker:        maxWorker,
		handlers:         handlers,
	}
}

func (e *Engine) newContext() *Context {
	return &Context{}
}

func (e *Engine) Run() error {
	if e.shuttingDown() {
		return ErrServerClosed
	}
	defer e.Close()

	for topic, subs := range e.tree {
		for subscriptionName := range subs {
			consumer, err := e.driver.Consumer(topic, subscriptionName)
			if err != nil {
				return err
			}
			if ok := e.trackConsumer(&consumer, true); !ok {
				return ErrServerClosed
			}
			go func() {
				defer e.trackConsumer(&consumer, false)
				for {
					if e.shuttingDown() {
						return
					}
					// TODO: receive mess
					// TODO: worker
				}
			}()
		}
	}

	return nil
}

func (e *Engine) trackConsumer(c *Consumer, add bool) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.consumers == nil {
		e.consumers = make(map[*Consumer]struct{})
	}

	if add {
		if e.shuttingDown() {
			return false
		}
		e.consumers[c] = struct{}{}
		e.consumerGroup.Add(1)
	} else {
		delete(e.consumers, c)
		e.consumerGroup.Done()
	}
	return true
}

func (e *Engine) stopConsumers() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	var err error
	for c := range e.consumers {
		if cerr := (*c).Stop(); cerr != nil && err == nil {
			err = cerr
		}
	}
	return err
}

func (e *Engine) Close() error {
	e.inShutdown.Store(true)

	var err error

	err = e.stopConsumers()
	e.consumerGroup.Wait()

	if derr := e.driver.Close(); derr != nil && err == nil {
		err = derr
	}

	return err
}

const shutdownPollIntervalMax = 500 * time.Millisecond

func (e *Engine) Shutdown(ctx context.Context) error {
	e.inShutdown.Store(true)

	var err error

	err = e.stopConsumers()
	e.consumerGroup.Wait()

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
		// TODO: check if there are any active worker
		if false {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			timer.Reset(nextPollInterval())
		}
	}

	if derr := e.driver.Close(); derr != nil && err == nil {
		err = derr
	}
	return err
}

func (e *Engine) shuttingDown() bool {
	return e.inShutdown.Load()
}
