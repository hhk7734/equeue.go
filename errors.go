package equeue

import "errors"

var (
	ErrServerClosed = errors.New("equeue: Server closed")

	ErrConsumerStoped = errors.New("equeue: Consumer stoped")
)
