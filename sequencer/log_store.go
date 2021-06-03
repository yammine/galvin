package sequencer

import (
	"context"

	"github.com/lni/dragonboat/v3/client"
	"github.com/lni/dragonboat/v3/statemachine"
)

// LogStore is an interface that our consensus store must implement
type LogStore interface {
	SyncPropose(context.Context, *client.Session, []byte) (statemachine.Result, error)
	SyncRead(ctx context.Context, clusterID uint64, query interface{}) (interface{}, error)
	GetNoOPSession(uint64) *client.Session
}
