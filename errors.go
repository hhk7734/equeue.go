package equeue

import (
	"errors"
	"fmt"
)

var (
	ErrServerClosed    = errors.New("equeue: Server closed")
	ErrConsumerStopped = errors.New("equeue: Consumer stoped")
)

type ErrorType uint64

const (
	ErrorTypePrivate ErrorType = 1 << 0
	ErrorTypeBind    ErrorType = 1 << 63
)

var _ error = &Error{}

type Error struct {
	Err  error
	Type ErrorType
	Meta any
}

func (msg *Error) SetType(flags ErrorType) *Error {
	msg.Type = flags
	return msg
}

func (msg *Error) SetMeta(data any) *Error {
	msg.Meta = data
	return msg
}

type equeueErrors []*Error

func (msg Error) Error() string {
	return fmt.Sprintf("err `%v`, meta `%v`", msg.Err, msg.Meta)
}
