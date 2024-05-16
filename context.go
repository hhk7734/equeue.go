package equeue

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/cloudevents/sdk-go/v2/event"
)

const abortIndex int8 = math.MaxInt8 >> 1

type ResultNackWithRedeliveryDelay struct {
	delay time.Duration
}

func (r ResultNackWithRedeliveryDelay) Error() string {
	return "nack with delay"
}

func (r ResultNackWithRedeliveryDelay) Delay() time.Duration {
	return r.delay
}

type Context struct {
	engine *Engine

	Request *Request

	handlers            HandlersChain
	index               int8
	nack                bool
	nackRedeliveryDelay time.Duration

	Errors equeueErrors
}

func (c *Context) reset() {
	c.handlers = nil
	c.index = -1
	c.nack = false
	c.nackRedeliveryDelay = 0

	c.Errors = c.Errors[:0]
}

func (c *Context) Publish(ctx context.Context, topic string, event event.Event) error {
	return c.engine.Publish(ctx, topic, event)
}

func (c *Context) Next() {
	c.index++
	for c.index < int8(len(c.handlers)) {
		c.handlers[c.index](c)
		c.index++
	}
}

func (c *Context) Abort() {
	c.index = abortIndex
}

func (c *Context) IsAborted() bool {
	return c.index >= abortIndex
}

func (c *Context) AbortWithNack() {
	c.Nack()
	c.Abort()
}

func (c *Context) AbortWithError(err error) {
	c.Error(err)
	c.Abort()
}

func (c *Context) Nack() {
	c.nack = true
}

func (c *Context) NackWithRedeliveryDelay(delay time.Duration) {
	c.nack = true
	c.nackRedeliveryDelay = delay
}

func (c *Context) IsNack() bool {
	return c.nack
}

func (c *Context) Error(err error) *Error {
	if err == nil {
		panic("err is nil")
	}

	var perr *Error
	ok := errors.As(err, &perr)
	if !ok {
		perr = &Error{
			Err:  err,
			Type: ErrorTypePrivate,
		}
	}

	c.Errors = append(c.Errors, perr)
	return perr
}

func (c *Context) Done() <-chan struct{} {
	return c.Request.Context().Done()
}
