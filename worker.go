package equeue

import "context"

type worker struct {
	engine       *Engine
	subscription *subscription
	message      Message
	cancelCtx    context.CancelFunc
}

func (w *worker) run() {
	defer w.engine.trackWorker(w, false)

	c := w.engine.pool.Get().(*Context)
	c.reset()
	c.Event = w.message.Event()
	c.handlers = w.subscription.handlers
	c.ctx, w.cancelCtx = context.WithCancel(context.Background())
	c.Next()
	if c.IsNack() {
		w.message.Nack()
	} else {
		w.message.Ack()
	}
	w.engine.pool.Put(c)
}

func (w *worker) cancel() {
	if w.cancelCtx != nil {
		w.cancelCtx()
	}
}
