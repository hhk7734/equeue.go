package equeue

type Consumer interface {
	Close() error
}
