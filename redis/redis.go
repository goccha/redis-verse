package redis

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/goccha/logging/log"
	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"
)

const (
	DefaultHost = "localhost:6379"
	Nil         = redis.Nil
)

func IsNil(err error) bool {
	return errors.Is(err, redis.Nil)
}

func Universal(ctx context.Context, f func(ctx context.Context, c redis.Cmdable) error, db ...int) (err error) {
	c := primary.Client
	cmdable := redis.Cmdable(c)
	var pipe redis.Pipeliner
	if primary.Db >= 0 && len(db) > 0 && db[0] != primary.Db {
		pipe = c.Pipeline()
		if cmd := pipe.Select(ctx, db[0]); cmd.Err() != nil {
			return cmd.Err()
		}
		cmdable = pipe
	}
	defer func() {
		if pipe != nil {
			if err = primary.Reset(ctx, pipe); err != nil {
				_, err = pipe.Exec(ctx)
			}
		}
	}()
	return f(ctx, cmdable)
}

func ReadOnly(ctx context.Context, f func(ctx context.Context, c redis.Cmdable) error, db ...int) (err error) {
	c := reader.Client
	cmdable := c
	var pipe redis.Pipeliner
	if reader.Db >= 0 && len(db) > 0 && db[0] != reader.Db {
		pipe = c.Pipeline()
		if cmd := pipe.Select(ctx, db[0]); cmd.Err() != nil {
			return cmd.Err()
		}
		cmdable = pipe
	}
	defer func() {
		if err = reader.Reset(ctx, pipe); err != nil {
			_, err = pipe.Exec(ctx)
		}
	}()
	return f(ctx, cmdable)
}

func Primary() redis.UniversalClient {
	return primary.Client
}
func Reader() redis.Cmdable {
	return reader.Client
}

func Ping(ctx context.Context) error {
	if cmd := Reader().Ping(ctx); cmd.Err() != nil {
		return cmd.Err()
	}
	return nil
}

var primary *PrimaryClient
var reader *ReaderClient

var _builder Builder

func Setup(ctx context.Context, b Builder) error {
	_builder = b
	return Refresh(ctx, b)
}

func WaitForActivation(ctx context.Context, waitMax ...int) error {
	ok := false
	tryMax := 20
	if len(waitMax) > 0 {
		tryMax = waitMax[0]
	}
	for i := 0; i < tryMax; i++ {
		if cmd := Primary().Ping(context.TODO()); cmd.Err() != nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		ok = true
		break
	}
	if !ok {
		return errors.New("time out")
	}
	return nil
}

func Refresh(ctx context.Context, b ...Builder) error {
	var builder Builder
	if len(b) > 0 {
		builder = b[0]
	} else {
		builder = _builder
	}
	if builder != nil {
		if w, r, err := builder.Build(ctx, DatabaseNumber()); err != nil {
			return err
		} else {
			primary = w
			reader = r

		}
		return nil
	}
	return errors.New("require builder")
}

type Builder interface {
	Build(ctx context.Context, db ...int) (*PrimaryClient, *ReaderClient, error)
}

func getReaderHost(host string) string {
	if host == DefaultHost {
		return host
	}
	index := strings.Index(host, ".")
	groupID := host[0:index]
	return fmt.Sprintf("%s-ro%s", groupID, host[index:])
}

type DefaultBuilder struct {
	ClusterEnable bool
	PrimaryHost   string
	ReaderHost    string
	TlsConfig     *tls.Config
}

type PrimaryClient struct {
	Client redis.UniversalClient
	Db     int
}

func (c PrimaryClient) Reset(ctx context.Context, cmdable redis.StatefulCmdable) error {
	if cmd := cmdable.Select(ctx, c.Db); cmd.Err() != nil {
		return cmd.Err()
	}
	return nil
}

type ReaderClient struct {
	Client redis.Cmdable
	Db     int
}

func (c ReaderClient) Reset(ctx context.Context, cmdable redis.StatefulCmdable) error {
	if cmd := cmdable.Select(ctx, c.Db); cmd.Err() != nil {
		return cmd.Err()
	}
	return nil
}

func (b *DefaultBuilder) Build(ctx context.Context, db ...int) (primary *PrimaryClient, reader *ReaderClient, err error) {
	if b.ClusterEnable {
		c := redis.NewClusterClient(&redis.ClusterOptions{
			Addrs: []string{b.PrimaryHost},
		})
		host, port := splitEndpoint(b.PrimaryHost)
		if err = redisotel.InstrumentTracing(c, redisotel.WithAttributes(semconv.NetPeerNameKey.String(host), semconv.NetPeerPortKey.String(port))); err != nil {
			return
		}
		primary = &PrimaryClient{Client: c, Db: -1}
		log.Info(ctx).Str("primary_endpoint", b.PrimaryHost).Send()
		reader = &ReaderClient{Client: c, Db: -1}
	} else {
		d := 0
		if len(db) > 0 {
			d = db[0]
		}
		c := redis.NewClient(&redis.Options{
			Addr:      b.PrimaryHost,
			DB:        d,
			TLSConfig: b.TlsConfig,
		})
		host, port := splitEndpoint(b.PrimaryHost)
		if err = redisotel.InstrumentTracing(c, redisotel.WithAttributes(semconv.NetPeerNameKey.String(host), semconv.NetPeerPortKey.String(port))); err != nil {
			return
		}
		primary = &PrimaryClient{Client: c, Db: d}
		log.Info(ctx).Str("primary_endpoint", b.PrimaryHost).Send()
		if b.PrimaryHost != b.ReaderHost {
			c = redis.NewClient(&redis.Options{
				Addr: b.ReaderHost,
				DB:   d,
			})
			host, port = splitEndpoint(b.ReaderHost)
			if err = redisotel.InstrumentTracing(c, redisotel.WithAttributes(semconv.NetPeerNameKey.String(host), semconv.NetPeerPortKey.String(port))); err != nil {
				return
			}
			reader = &ReaderClient{
				Client: c,
				Db:     d,
			}
			log.Info(ctx).Str("reader_endpoint", b.ReaderHost).Send()
		} else {
			reader = &ReaderClient{
				Client: primary.Client,
				Db:     primary.Db,
			}
		}
	}
	return
}

func splitEndpoint(endpoint string) (host, port string) {
	v := strings.Split(endpoint, ":")
	switch len(v) {
	case 0:
		return "localhost", "6379"
	case 1:
		return endpoint, "6379"
	default:
		return v[0], v[1]
	}
}
