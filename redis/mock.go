package redis

import (
	"context"

	"github.com/go-redis/redismock/v8"
)

type MockBuilder struct {
	mock redismock.ClientMock
}

func (b *MockBuilder) Build(ctx context.Context, db ...int) (primary *PrimaryClient, reader *ReaderClient, err error) {
	c, mock := redismock.NewClientMock()
	primary = &PrimaryClient{Client: c, Db: 0}
	reader = &ReaderClient{
		Client: primary.Client,
		Db:     primary.Db,
	}
	b.mock = mock
	return
}

func (b *MockBuilder) Mock() redismock.ClientMock {
	return b.mock
}
