package raft

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/yammine/galvin/sequencer"

	sm "github.com/lni/dragonboat/v3/statemachine"
)

const (
	UpdateSuccess = iota
	UpdateFail
)

// Once a batch is committed we need to assign the serial ID of each transaction
// within the batch and commit it to this log.

type InMemory struct {
	clusterID uint64
	nodeID    uint64

	NextTxnID uint64
	// Very WIP lol
	Transactions [][]sequencer.Transaction
}

func NewInMemory(c, n uint64) sm.IStateMachine {
	return &InMemory{
		clusterID: c,
		nodeID:    n,
	}
}

// Update updates the SM. Ideally we'll have a durable storage for this log, let's just keep it all in memory for now.
func (i *InMemory) Update(bytes []byte) (sm.Result, error) {
	batch := &sequencer.Batch{}
	_ = json.Unmarshal(bytes, batch)

	for idx := range batch.Transactions {
		batch.Transactions[idx].ID = i.NextTxnID
		i.NextTxnID++
	}

	i.Transactions = append(i.Transactions, batch.Transactions)

	return sm.Result{Value: UpdateSuccess}, nil
}

func (i *InMemory) Lookup(q interface{}) (interface{}, error) {
	idx := q.(int)
	b := i.Transactions[idx]

	return b, nil
}

func (i *InMemory) SaveSnapshot(w io.Writer, c sm.ISnapshotFileCollection, done <-chan struct{}) error {
	b, err := json.Marshal(i)
	if err != nil {
		return fmt.Errorf("statemachine save snapshot: %w", err)
	}

	_, err = w.Write(b)
	return err
}

func (i *InMemory) RecoverFromSnapshot(r io.Reader, f []sm.SnapshotFile, done <-chan struct{}) error {
	var err error
	var b []byte

	if b, err = io.ReadAll(r); err != nil {
		return fmt.Errorf("recover from snapshot readall: %w", err)
	}
	if err = json.Unmarshal(b, i); err != nil {
		return fmt.Errorf("recover from snapshot unmarshal: %w", err)
	}

	return nil
}

func (i *InMemory) Close() error {
	// Since this is all in memory there's nothing to do.
	return nil
}
