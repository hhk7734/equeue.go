package equeue

type Router interface {
	Routes
	Group(string, ...HandlerFunc) *RouterGroup
}

type Routes interface {
	Use(handlers ...HandlerFunc) Routes

	Handle(topic string, subscriptionName string, handlers ...HandlerFunc) Routes
}

type RouterGroup struct {
	Handlers  HandlersChain
	baseTopic string
	engine    *Engine
	root      bool
}

var _ Router = new(RouterGroup)

func (g *RouterGroup) Use(middleware ...HandlerFunc) Routes {
	g.Handlers = append(g.Handlers, middleware...)
	return g.returnObj()
}

func (g *RouterGroup) Group(relativeTopic string, handlers ...HandlerFunc) *RouterGroup {
	return &RouterGroup{
		Handlers:  g.combineHandlers(handlers),
		baseTopic: g.absoluteTopic(relativeTopic),
		engine:    g.engine,
	}
}

func (g *RouterGroup) Handle(relativeTopic string, subscriptionName string, handlers ...HandlerFunc) Routes {
	g.engine.addRoute(g.absoluteTopic(relativeTopic), subscriptionName, g.combineHandlers(handlers))
	return g.returnObj()
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

func (g *RouterGroup) absoluteTopic(relativeTopic string) string {
	return g.baseTopic + relativeTopic
}

func (g *RouterGroup) returnObj() Routes {
	if g.root {
		return g.engine
	}
	return g
}
