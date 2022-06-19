package aws

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/elasticache"
	"github.com/goccha/redis-verse/redis"
	"net/http"
	"net/url"
	"os"
)

type ApiBuilder struct {
	ReplicationGroupID string
	Cfg                aws.Config
}

func (b *ApiBuilder) getConfig() (cfg aws.Config, err error) {
	if v, ok := os.LookupEnv("AWS_ELASTICACHE_API_PROXY"); ok {
		cfg = b.Cfg.Copy()
		cfg.HTTPClient = &http.Client{
			Transport: &http.Transport{
				Proxy: func(*http.Request) (*url.URL, error) {
					return url.Parse(v)
				},
			},
		}
	} else {
		cfg = b.Cfg
	}
	return
}

func (b *ApiBuilder) Build(ctx context.Context, db ...int) (primary *redis.PrimaryClient, reader *redis.ReaderClient, err error) {
	var cfg aws.Config
	if cfg, err = b.getConfig(); err != nil {
		return nil, nil, err
	}
	cli := elasticache.NewFromConfig(cfg)
	if out, err := cli.DescribeReplicationGroups(context.TODO(), &elasticache.DescribeReplicationGroupsInput{
		ReplicationGroupId: aws.String(b.ReplicationGroupID),
	}); err != nil {
		return nil, nil, err
	} else {
		for _, g := range out.ReplicationGroups {
			if g.ClusterEnabled != nil && *g.ClusterEnabled {
				ep := g.ConfigurationEndpoint
				host := fmt.Sprintf("%s:%d", *ep.Address, ep.Port)
				builder := &redis.DefaultBuilder{
					ClusterEnable: true,
					PrimaryHost:   host,
				}
				return builder.Build(ctx, db...)
			} else {
				for _, n := range g.NodeGroups {
					if *n.Status == "available" {
						ep := n.PrimaryEndpoint
						primaryHost := fmt.Sprintf("%s:%d", *ep.Address, ep.Port)
						ep = n.ReaderEndpoint
						readerHost := fmt.Sprintf("%s:%d", *ep.Address, ep.Port)
						builder := &redis.DefaultBuilder{
							ClusterEnable: false,
							PrimaryHost:   primaryHost,
							ReaderHost:    readerHost,
						}
						return builder.Build(ctx, db...)
					}
				}
			}
		}
	}
	return nil, nil, errors.New("elasticache: Not found replication groups")
}
