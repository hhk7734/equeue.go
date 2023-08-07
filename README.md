## equeue.go

### ZapxLoggerMiddleware

```go
package middleware

import (
	"fmt"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/hhk7734/equeue.go"
	"github.com/hhk7734/zapx.go"
	"go.uber.org/zap"
)

type ZapxLoggerMiddleware struct{}

func (z *ZapxLoggerMiddleware) Logger(c *equeue.Context) {
	start := time.Now()
	c.Next()

	ctx := zapx.WithFields(c.Request.Context(),
		zap.String("ce_id", c.Request.Event.ID()),
		zap.String("ce_type", c.Request.Event.Type()),
		zap.String("ce_source", c.Request.Event.Source()),
		zap.String("ce_time", c.Request.Event.Time().Format(time.RFC3339)),
		zap.Duration("latency", time.Since(start)))
	logger := zapx.Ctx(ctx)

	if len(c.Errors) > 0 {
		for _, e := range c.Errors {
			logger.Error("internal error", zap.Error(e))
		}
	} else {
		logger.Info("logger")
	}
}

func (z *ZapxLoggerMiddleware) Recovery(c *equeue.Context) {
	defer func() {
		if err := recover(); err != nil {
			c.AbortWithError(fmt.Errorf("%v\n%s", err, string(debug.Stack())))
		}
	}()
	c.Next()
}
```

```go
	// r := equeue.New(...)
	// ...

	zapxLogger := &middleware.ZapxLoggerMiddleware{}

	r.Use(zapxLogger.Logger)
	r.Use(zapxLogger.Recovery)
```