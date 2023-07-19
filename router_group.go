package equeue

type Router interface {
	Routes
	Group(string, ...HandlerFunc) *RouterGroup
}

type Routes interface {
	Use(handlers ...HandlerFunc) Routes

	HandleWithTopic(topic string, subscriptionName string, handlers ...HandlerFunc) Routes
	Handle(subscriptionName string, handlers ...HandlerFunc) Routes
}

type RouterGroup struct {
	Handlers HandlersChain
	topic    string
	engine   *Engine
	root     bool
}

var _ Router = new(RouterGroup)

func (g *RouterGroup) Use(middleware ...HandlerFunc) Routes {
	g.Handlers = append(g.Handlers, middleware...)
	return g.returnObj()
}

func (g *RouterGroup) Group(topic string, handlers ...HandlerFunc) *RouterGroup {
	return &RouterGroup{
		Handlers: g.combineHandlers(handlers),
		topic:    g.topicIfNil(topic),
		engine:   g.engine,
	}
}

func (g *RouterGroup) HandleWithTopic(topic string, subscriptionName string, handlers ...HandlerFunc) Routes {
	handlers = g.combineHandlers(handlers)
	g.engine.addRoute(g.topicIfNil(topic), subscriptionName, handlers)
	return g.returnObj()
}

func (g *RouterGroup) Handle(subscriptionName string, handlers ...HandlerFunc) Routes {
	return g.HandleWithTopic(g.topic, subscriptionName, handlers...)
}

func (g *RouterGroup) combineHandlers(handlers HandlersChain) HandlersChain {
	finalSize := len(g.Handlers) + len(handlers)
	if finalSize >= int(abortIndex) {
		panic("too many handlers")
	}
	mergedHandlers := make(HandlersChain, finalSize)
	copy(mergedHandlers, g.Handlers)
	copy(mergedHandlers[len(g.Handlers):], handlers)
	return mergedHandlers
}

func (g *RouterGroup) topicIfNil(topic string) string {
	if topic == "" {
		if g.topic == "" {
			panic("topic can not be empty")
		}
		return g.topic
	}
	return topic
}

func (g *RouterGroup) returnObj() Routes {
	if g.root {
		return g.engine
	}
	return g
}
