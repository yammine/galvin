package scheduler

import (
	"context"
	"fmt"

	"github.com/yammine/galvin/storage"

	"github.com/yammine/galvin/sequencer"
)

type Sequential struct {
	in      chan []sequencer.Transaction
	storage storage.Storage
}

var _ sequencer.Scheduler = (*Sequential)(nil)

func NewSequential(s storage.Storage) *Sequential {
	return &Sequential{
		in:      make(chan []sequencer.Transaction),
		storage: s,
	}
}

func (s Sequential) Run(ctx context.Context) {
	for {
		select {
		case b := <-s.in:
			fmt.Println("batch received by scheduler ", b)
		case <-ctx.Done():
			return
		}
	}
}

func (s Sequential) BatchInput() chan []sequencer.Transaction {
	return s.in
}
