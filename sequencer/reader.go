package sequencer

import (
	"context"
	"fmt"
	"time"
)

// Reader ...
type Reader struct {
	s           Store
	scheduler   Scheduler
	batchNumber uint64
}

type Scheduler interface {
	BatchInput() chan []Transaction
}

func NewReader(s Store, scheduler Scheduler) *Reader {
	return &Reader{
		s:         s,
		scheduler: scheduler,
	}
}

func (r *Reader) Run(ctx context.Context) {
	for {
		subCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		// Read a batch from our log store
		b, err := r.s.SyncRead(subCtx, 1, r.batchNumber)
		cancel()

		if err != nil {
			fmt.Println("Error fetching batch", err)
			time.Sleep(time.Second)
			continue
		}

		select {
		case <-ctx.Done():
			fmt.Println("ctx cancelled, terminating loop")
			return
		case r.scheduler.BatchInput() <- b.([]Transaction):
			r.batchNumber++
		}
	}
}
