package scheduler

import (
	"context"
	"fmt"

	"github.com/yammine/galvin/sequencer"
)

type Sequential struct {
	in      chan []sequencer.Transaction
	storage Storage
	publisher
}

type publisher interface {
	Publish(string, interface{})
	CloseTopic(string)
}

var _ sequencer.Scheduler = (*Sequential)(nil)

func (s Sequential) Run(ctx context.Context) {
	for {
		select {
		case b := <-s.in:
			fmt.Println("batch received by scheduler ", b)
			for i := range b {
				tx := b[i]
				s.publisher.Publish(tx.Ref.String(), fmt.Sprintf("Processed transaction: %d", tx.ID))
				s.publisher.CloseTopic(tx.Ref.String())
			}
		case <-ctx.Done():
			return
		}
	}
}

func NewSequential(s Storage, ps publisher) *Sequential {
	return &Sequential{
		in:        make(chan []sequencer.Transaction),
		storage:   s,
		publisher: ps,
	}
}

func (s Sequential) BatchInput() chan []sequencer.Transaction {
	return s.in
}
