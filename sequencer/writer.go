package sequencer

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// Writer is the acceptor of all Galvin input.
// It collects inputs for each epoch & persists the batch in a globally consistent order.
type Writer struct {
	store LogStore
	input chan Transaction
	epoch time.Duration
}

// NewWriter returns a new Writer struct
func NewWriter(store LogStore, config ...Config) *Writer {
	ch := make(chan Transaction)
	s := &Writer{store: store, input: ch, epoch: time.Second}

	return s
}

func consumptionLoop(ctx context.Context, b *Batch, in chan Transaction) {
	for {
		select {
		case tx, ok := <-in:
			if !ok {
				fmt.Println("Channel is closed.")
				break
			}
			b.Add(tx)
		case <-ctx.Done():
			fmt.Println("ctx cancelled, terminating loop")
			return
		}
	}
}

// Run begins the consumption loop of Writer
func (s *Writer) Run(ctx context.Context) {
	batch := &Batch{}
	timer := time.NewTicker(s.epoch)

	defer timer.Stop()
	defer batch.Flush(ctx, s.store)

	// Run the loop to add things to the batch
	go consumptionLoop(ctx, batch, s.input)

	for {
		select {
		case <-timer.C:
			batch.Flush(ctx, s.store)
		case <-ctx.Done():
			fmt.Println("ctx cancelled, terminating loop")
			return
		}
	}

}

// SubmitTransaction ...
func (s *Writer) SubmitTransaction(ctx context.Context, txn Transaction) {
	s.input <- txn
}

// Config ...
type Config struct {
	Reader        bool
	EpochDuration time.Duration
}

// Add ...
func (b *Batch) Add(t Transaction) {
	b.Lock()
	defer b.Unlock()

	b.Transactions = append(b.Transactions, t)
}

// Flush ...
func (b *Batch) Flush(ctx context.Context, s LogStore) {
	b.Lock()
	defer b.Unlock()
	if len(b.Transactions) == 0 {
		return
	}
	fmt.Println("Flushing the current batch")

	// build batch bytes
	// TODO: Handle errors
	bytes, _ := json.Marshal(b)

	subCtx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()

	// TODO: replace the int there with a configured cluster id
	cs := s.GetNoOPSession(1)
	sr, err := s.SyncPropose(subCtx, cs, bytes)
	if err != nil {
		fmt.Println("error: ", err)
		return
	}

	// Clearing the batch since it has flushed successfully
	b.Transactions = make([]Transaction, 0)
	// Incrementing the batch number
	b.Number++
	fmt.Println("StateMachine Result", sr)
}

type Batch struct {
	sync.Mutex

	Number       uint64
	Transactions []Transaction
}

type Transaction struct {
	// Globally unique transaction id
	ID uint64
	// Specifies which stored procedure to invoke at execution time.
	Type string
	// Arguments to be passed when invoking the stored procedure to execute this
	// transaction. Args is a serialized protocol message. The client and backend
	// application code is assumed to know how to interpret this protocol message
	// based on Type.
	Args []byte

	// True if a transaction is known to span multiple nodes.
	MultiPartition bool

	// Keys of objects read (but not modified) by this transaction.
	ReadSet []string
	// Keys of objects modified (but not read) by this transaction.
	WriteSet []string
	// Keys of objects both read & modified by this transaction.
	ReadWriteSet []string

	// Nodes that will participate as Readers and Writers in this transaction.
	Readers []string
	Writers []string
}
