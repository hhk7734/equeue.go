package equeue

type Driver interface {
	Consumer(topic string, subscriptionName string) (Consumer, error)
	Close() error
}
