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

type subsTree map[string]map[string]*subscription

type Engine struct {
	RouterGroup

	driver Driver

	inShutdown atomic.Bool

	pool sync.Pool
	tree subsTree

	mu            sync.Mutex
	consumers     map[*Consumer]struct{}
	consumerGroup sync.WaitGroup

	activeWorkers map[*worker]struct{}
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
		e.tree[topic] = make(map[string]*subscription)
	}

	if _, ok := e.tree[topic][subscriptionName]; ok {
		panic("duplicated subscription")
	}

	e.tree[topic][subscriptionName] = &subscription{
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
		for subName, sub := range subs {
			consumer, err := e.driver.Consumer(topic, subName)
			if err != nil {
				return err
			}
			if ok := e.trackConsumer(&consumer, true); !ok {
				return ErrServerClosed
			}

			s := sub
			go func() {
				defer e.trackConsumer(&consumer, false)
				for {
					if e.shuttingDown() {
						return
					}
					msg, err := consumer.Receive()
					if err != nil {
						// TODO: handle error
						return
					}
					w := e.newWorker(s, msg)
					if ok := e.trackWorker(w, true); !ok {
						return
					}
					go w.run()
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

func (e *Engine) newWorker(s *subscription, m Message) *worker {
	return &worker{engine: e, subscription: s, message: m}
}

func (e *Engine) trackWorker(w *worker, add bool) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.activeWorkers == nil {
		e.activeWorkers = make(map[*worker]struct{})
	}

	if add {
		if e.shuttingDown() {
			return false
		}
		e.activeWorkers[w] = struct{}{}
	} else {
		delete(e.activeWorkers, w)
	}
	return true
}

func (e *Engine) cancelWorkers() {
	e.mu.Lock()
	defer e.mu.Unlock()

	for w := range e.activeWorkers {
		w.cancel()
	}
}

func (e *Engine) Close() error {
	e.inShutdown.Store(true)

	var err error

	err = e.stopConsumers()
	e.consumerGroup.Wait()

	e.cancelWorkers()
	time.Sleep(1 * time.Second)

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

	e.cancelWorkers()

	// Wait for active workers.
	pollIntervalBase := 10 * time.Millisecond
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
		e.mu.Lock()
		if len(e.activeWorkers) == 0 {
			e.mu.Unlock()
			break
		}
		e.mu.Unlock()
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
