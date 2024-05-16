package equeue

import (
	"context"
	"errors"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/cloudevents/sdk-go/v2/binding/format"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/cloudevents/sdk-go/v2/protocol"
)

type HandlerFunc func(*Context)

type HandlersChain []HandlerFunc

type OptionFunc func(*Engine)

func WithForcedPublicationSource(s string) OptionFunc {
	return func(e *Engine) {
		e.forcedPublicationSource = s
	}
}

func WithForcedPublicationFormat(f format.Format) OptionFunc {
	return func(e *Engine) {
		e.forcedPublicationFormat = f
	}
}

func WithForcedSubscriptionFormat(f format.Format) OptionFunc {
	return func(e *Engine) {
		e.forcedSubscriptionFormat = f
	}
}

func New(driver Driver, opts ...OptionFunc) *Engine {
	engine := &Engine{
		RouterGroup: RouterGroup{
			root: true,
		},
		driver:                       driver,
		tree:                         make(subsTree),
		consumers:                    make(map[*Consumer]struct{}),
		subscriptionActiveWorkersMap: make(map[*subscription]map[*worker]struct{}),
	}
	engine.RouterGroup.engine = engine
	engine.pool.New = func() interface{} {
		return engine.newContext()
	}

	for _, opt := range opts {
		opt(engine)
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

	driver                   Driver
	forcedPublicationSource  string
	forcedPublicationFormat  format.Format
	forcedSubscriptionFormat format.Format

	inShutdown atomic.Bool

	pool sync.Pool
	tree subsTree

	mu            sync.Mutex
	consumers     map[*Consumer]struct{}
	consumerGroup sync.WaitGroup

	subscriptionActiveWorkersMap map[*subscription]map[*worker]struct{}
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

	sub := &subscription{
		topic:            topic,
		subscriptionName: subscriptionName,
		maxWorker:        maxWorker,
		handlers:         handlers,
	}

	e.tree[topic][subscriptionName] = sub
	if sub.maxWorker <= 0 {
		e.subscriptionActiveWorkersMap[sub] = make(map[*worker]struct{})
	} else {
		e.subscriptionActiveWorkersMap[sub] = make(map[*worker]struct{}, sub.maxWorker)
	}
}

func (e *Engine) newContext() *Context {
	return &Context{engine: e}
}

func (e *Engine) Publish(ctx context.Context, topic string, event event.Event) error {
	if e.forcedPublicationSource != "" {
		event.SetSource(e.forcedPublicationSource)
	}
	if err := event.Validate(); err != nil {
		return err
	}
	if e.forcedPublicationFormat != nil {
		ctx = binding.UseFormatForEvent(ctx, e.forcedPublicationFormat)
	}
	return e.driver.Send(ctx, topic, (*binding.EventMessage)(&event))
}

func (e *Engine) Run() error {
	if e.shuttingDown() {
		return ErrServerClosed
	}
	defer e.Close()

	consumerErr := make(chan error, 1)

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
				defer func() {
					// TODO: log error
					recover()
				}()

				for {
					if e.shuttingDown() {
						return
					}
					if s.maxWorker > 0 && e.countActiveWorkers(s) >= s.maxWorker {
						time.Sleep(2*time.Second + time.Duration(rand.Intn(int(time.Second))))
						continue
					}

					ctx := context.Background()
					if e.forcedSubscriptionFormat != nil {
						ctx = withForcedReceivedMessageFormat(ctx, e.forcedSubscriptionFormat)
					}
					msg, err := consumer.Receive(ctx)
					switch {
					case errors.Is(err, ErrConsumerStopped):
						return
					case err != nil:
						consumerErr <- err
						close(consumerErr)
						return
					}
					w := e.newWorker(s, msg)
					if ok := e.trackWorker(w, true); !ok {
						w.message.Finish(protocol.ResultNACK)
						return
					}
					go w.run()
				}
			}()
		}
	}

	return <-consumerErr
}

func (e *Engine) trackConsumer(c *Consumer, add bool) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

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

func (e *Engine) newWorker(s *subscription, m binding.Message) *worker {
	return &worker{engine: e, subscription: s, message: m}
}

func (e *Engine) trackWorker(w *worker, add bool) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	if add {
		if e.shuttingDown() {
			return false
		}
		e.subscriptionActiveWorkersMap[w.subscription][w] = struct{}{}
	} else {
		delete(e.subscriptionActiveWorkersMap[w.subscription], w)
	}
	return true
}

func (e *Engine) countActiveWorkers(s *subscription) int {
	e.mu.Lock()
	defer e.mu.Unlock()

	return len(e.subscriptionActiveWorkersMap[s])
}

func (e *Engine) countActiveWorkersAll() int {
	e.mu.Lock()
	defer e.mu.Unlock()

	var count int
	for _, workers := range e.subscriptionActiveWorkersMap {
		count += len(workers)
	}
	return count
}

func (e *Engine) cancelWorkers() {
	e.mu.Lock()
	defer e.mu.Unlock()

	for _, workers := range e.subscriptionActiveWorkersMap {
		for w := range workers {
			w.cancel()
		}
	}
}

type worker struct {
	engine       *Engine
	subscription *subscription
	message      binding.Message
	cancelCtx    context.CancelFunc
}

func (w *worker) run() {
	defer w.engine.trackWorker(w, false)

	c := w.engine.pool.Get().(*Context)
	c.reset()

	var ctx context.Context
	if mctx, ok := w.message.(binding.MessageContext); ok {
		ctx = mctx.Context()
	} else {
		ctx = context.Background()
	}
	ctx, w.cancelCtx = context.WithCancel(ctx)

	var err error
	e, err := binding.ToEvent(ctx, w.message)
	if err != nil {
		_, filename, line, _ := runtime.Caller(0)
		c.Error(err).SetType(ErrorTypeBind).SetMeta(H{
			"filename": filename,
			"line":     line + 1})

		ev := event.New()
		e = &ev
	}
	c.Request = &Request{Event: e, ctx: ctx}
	c.handlers = w.subscription.handlers
	c.Next()

	if c.IsNack() {
		w.message.Finish(protocol.ResultNACK)
	} else {
		w.message.Finish(protocol.ResultACK)
	}

	w.engine.pool.Put(c)
}

func (w *worker) cancel() {
	if w.cancelCtx != nil {
		w.cancelCtx()
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
		if e.countActiveWorkersAll() == 0 {
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
