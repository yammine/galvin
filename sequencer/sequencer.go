package sequencer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/lni/dragonboat/v3/client"
	"github.com/lni/dragonboat/v3/statemachine"
)

var (
	defaults Config = Config{
		EpochDuration: 10,
		Reader:        false,
	}
)

// Sequencer is the acceptor of all Galvin input.
// It collects inputs for each epoch & persists the batch in a globally consistent order.
type Sequencer struct {
	store  Store
	reader bool
	input  chan Transaction
	errc   chan error
	epoch  time.Duration
}

// New returns a new Sequencer struct
func New(store Store, config ...Config) *Sequencer {
	ch := make(chan Transaction)
	errc := make(chan error, 1)
	s := &Sequencer{store: store, input: ch, errc: errc}

	return s
}

// Run begins the consumption loop of Sequencer
func (s *Sequencer) Run(ctx context.Context) {
	var batch *Batch
	defer close(s.input)
	defer batch.Flush(ctx, s.store, s.errc)

	for {
		start := time.Now()
		batch = &Batch{}
		// Add transactions to the Batch until the epoch's duration has lapsed
		for time.Now().Before(start.Add(s.epoch)) {
			select {
			case txn, ok := <-s.input:
				if ok {
					fmt.Printf("txn: %+v\n", txn)
					batch.Add(&txn)
				}
			case err := <-s.errc:
				fmt.Println("Error happened", err)
			case <-ctx.Done():
				fmt.Println("Exiting")
				return
			}
		}

		go batch.Flush(ctx, s.store, s.errc)
	}
}

// SubmitTransaction ...
func (s *Sequencer) SubmitTransaction(ctx context.Context, txn Transaction) {
	s.input <- txn
}

// Config ...
type Config struct {
	Reader        bool
	EpochDuration time.Duration
}

// Batch is a batch of transactions
type Batch struct {
	txns []*Transaction

	sync.Mutex
}

// Add ...
func (b *Batch) Add(t *Transaction) {
	b.Lock()
	defer b.Unlock()

	b.txns = append(b.txns, t)
}

// Flush ...
func (b *Batch) Flush(ctx context.Context, s Store, errc chan error) {
	if len(b.txns) == 0 {
		return
	}
	// build batch bytes
	var bytes []byte
	for i := range b.txns {
		bytes = append(bytes, b.txns[i].Raw...)
	}

	// replace the int there with a configured cluster id
	cs := s.GetNoOPSession(1)
	sr, err := s.SyncPropose(ctx, cs, bytes)
	if err != nil {
		errc <- err
		return
	}
	fmt.Println("StateMachine Result", sr)
}

// Transaction ...
type Transaction struct {
	Raw string
}

// Store is an interface that our consensus store must implement
type Store interface {
	SyncPropose(context.Context, *client.Session, []byte) (statemachine.Result, error)
	GetNoOPSession(uint64) *client.Session
}
