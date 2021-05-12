package raft

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/big"

	sm "github.com/lni/dragonboat/v3/statemachine"
)

// ExampleStateMachine is the IStateMachine implementation used in the
// helloworld example.
// See https://github.com/lni/dragonboat/blob/master/statemachine/rsm.go for
// more details of the IStateMachine interface.
type ExampleStateMachine struct {
	ClusterID uint64 `json:"-"`
	NodeID    uint64 `json:"-"`

	Count           uint64
	NextBatchNumber *big.Int
}

var _ sm.IStateMachine = (*ExampleStateMachine)(nil)

// NewExampleStateMachine creates and return a new ExampleStateMachine object.
func NewExampleStateMachine(clusterID uint64, nodeID uint64) sm.IStateMachine {
	return &ExampleStateMachine{
		ClusterID:       clusterID,
		NodeID:          nodeID,
		Count:           0,
		NextBatchNumber: big.NewInt(0),
	}
}

// Lookup performs local lookup on the ExampleStateMachine instance. In this example,
// we always return the Count value as a little endian binary encoded byte
// slice.
func (s *ExampleStateMachine) Lookup(query interface{}) (interface{}, error) {
	result := make([]byte, 8)
	binary.LittleEndian.PutUint64(result, s.Count)
	return result, nil
}

// Update updates the object using the specified committed raft entry.
func (s *ExampleStateMachine) Update(data []byte) (sm.Result, error) {
	// in this example, we print out the following hello world message for each
	// incoming update request. we also increase the counter by one to remember
	// how many updates we have applied
	s.Count++
	s.NextBatchNumber = s.NextBatchNumber.Add(s.NextBatchNumber, big.NewInt(1))
	fmt.Printf("from ExampleStateMachine.Update(), msg: %s, count:%d, next_batch_number: %s\n", string(data), s.Count, s.NextBatchNumber.String())
	return sm.Result{Value: uint64(len(data))}, nil
}

// SaveSnapshot saves the current IStateMachine state into a snapshot using the
// specified io.Writer object.
func (s *ExampleStateMachine) SaveSnapshot(w io.Writer, fc sm.ISnapshotFileCollection, done <-chan struct{}) error {
	b, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("save snapshot json marshal: %w", err)
	}
	_, err = w.Write(b)
	return err
}

// RecoverFromSnapshot recovers the state using the provided snapshot.
func (s *ExampleStateMachine) RecoverFromSnapshot(r io.Reader, files []sm.SnapshotFile, done <-chan struct{}) error {
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return fmt.Errorf("readall: %w", err)
	}

	if err := json.Unmarshal(data, s); err != nil {
		return fmt.Errorf("recover from snapshot unmarshal json: %w", err)
	}

	return nil
}

// Close closes the IStateMachine instance. There is nothing for us to cleanup
// or release as this is a pure in memory data store. Note that the Close
// method is not guaranteed to be called as node can crash at any time.
func (s *ExampleStateMachine) Close() error { return nil }
