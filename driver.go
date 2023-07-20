package equeue

type Driver interface {
	Publisher

	Consumer(topic string, subscriptionName string, maxAckPending int) (Consumer, error)
	Close() error
}
