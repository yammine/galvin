package scheduler

import "context"

type SetOpts struct {
	Expires bool
	TTL     uint64
}

type Storage interface {
	Get(ctx context.Context, key string) (val string, err error)
	Set(ctx context.Context, key, val string, opts *SetOpts) error
	Delete(ctx context.Context, key string) (val string, err error)
}
