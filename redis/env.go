package redis

import (
	"context"
	"crypto/tls"

	"github.com/goccha/envar"
)

func init() {
	_env = &Env{}
	if err := envar.Bind(_env); err != nil {
		panic(err)
	}
}

type EnvBuilder struct{}

func (b *EnvBuilder) Build(ctx context.Context, db ...int) (primary *PrimaryClient, reader *ReaderClient, err error) {
	host := PrimaryEndpoint()
	readerHost := ReaderEndpoint()
	if readerHost == "" {
		readerHost = getReaderHost(host)
	}
	builder := &DefaultBuilder{
		ClusterEnable: ClusterEnable(),
		PrimaryHost:   host,
		ReaderHost:    readerHost,
	}
	if TlsEnable() {
		builder.TlsConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
			ServerName: ServerName(),
		}
	}
	return builder.Build(ctx, db...)
}

var _env *Env

type Env struct {
	RedisPrimaryEndpoint string `envar:"REDIS_PRIMARY_ENDPOINT;default=localhost:6379"`
	RedisReaderEndpoint  string `envar:"REDIS_READER_ENDPOINT"`
	RedisClusterEnable   bool   `envar:"REDIS_CLUSTER_ENABLE"`
	RedisDatabaseNumber  int    `envar:"REDIS_DATABASE_NUMBER;default=0"`
	TlsEnable            bool   `envar:"REDIS_TLS_ENABLE"`
	RedisServerName      string `envar:"REDIS_SERVER_NAME"`
}

func PrimaryEndpoint() string {
	return _env.RedisPrimaryEndpoint
}

func ReaderEndpoint() string {
	return _env.RedisReaderEndpoint
}

func ClusterEnable() bool {
	return _env.RedisClusterEnable
}

func DatabaseNumber() int {
	return _env.RedisDatabaseNumber
}

func TlsEnable() bool {
	return _env.TlsEnable
}

func ServerName() string {
	return _env.RedisServerName
}
