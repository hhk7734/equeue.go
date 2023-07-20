package equeue

import (
	"math"

	"github.com/cloudevents/sdk-go/v2/event"
)

const abortIndex int8 = math.MaxInt8 >> 1

type Context struct {
	Event *event.Event

	handlers HandlersChain
	index    int8
	nack     bool

	Errors []error
}

func (c *Context) reset() {
	c.handlers = nil
	c.index = -1

	c.Errors = c.Errors[:0]
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

func (c *Context) IsNack() bool {
	return c.nack
}

func (c *Context) Error(err error) {
	if err == nil {
		panic("err is nil")
	}

	c.Errors = append(c.Errors, err)
}
