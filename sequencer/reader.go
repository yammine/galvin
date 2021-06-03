package sequencer

import (
	"context"
	"log"
	"time"
)

// Reader ...
type Reader struct {
	s           LogStore
	scheduler   Scheduler
	batchNumber uint64
}

type Scheduler interface {
	BatchInput() chan []Transaction
}

func NewReader(s LogStore, scheduler Scheduler) *Reader {
	return &Reader{
		s:         s,
		scheduler: scheduler,
	}
}

func (r *Reader) Run(ctx context.Context) {
	reads := 0
	for {
		subCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		// Read a batch from our log store
		b, err := r.s.SyncRead(subCtx, 1, r.batchNumber)
		cancel()
		reads++

		if err != nil {
			if reads%10 == 0 {
				log.Println("Error fetching batch", err)
			}
			time.Sleep(time.Second)
			continue
		}

		select {
		case <-ctx.Done():
			log.Println("ctx cancelled, terminating loop")
			return
		case r.scheduler.BatchInput() <- b.([]Transaction):
			r.batchNumber++
		}
	}
}
