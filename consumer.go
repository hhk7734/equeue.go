package equeue

type Consumer interface {
	Stop() error
}
