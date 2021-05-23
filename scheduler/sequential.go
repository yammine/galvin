package scheduler

import (
	"context"
	"fmt"

	"github.com/yammine/galvin/sequencer"
)

type Sequential struct {
	in chan []sequencer.Transaction
}

var _ sequencer.Scheduler = (*Sequential)(nil)

func NewSequential() *Sequential {
	return &Sequential{
		in: make(chan []sequencer.Transaction),
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