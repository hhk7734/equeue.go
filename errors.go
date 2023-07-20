package equeue

import "errors"

var (
	ErrServerClosed = errors.New("equeue: Server closed")

	ErrConsumerStopped = errors.New("equeue: Consumer stoped")
)
