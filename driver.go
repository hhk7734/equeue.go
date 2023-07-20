package equeue

type Driver interface {
	Consumer(topic string, subscriptionName string, maxAckPending int) (Consumer, error)
	Close() error
}
