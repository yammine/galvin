package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jessevdk/go-flags"
	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/logger"
	"github.com/lni/goutils/syncutil"
)

type nodeOpts struct {
	NodeID uint64 `short:"n" long:"node_id" description:"Raft Node ID"`
	Addr   string `short:"h" long:"node_host" description:"Node Host Address"`
	Join   bool   `short:"j" long:"join" description:"Set if joining a new node."`
}

const (
	// Only one Raft cluster for now.
	clusterID uint64 = 1
)

var (
	initialClusterMembers = map[uint64]string{
		1: "localhost:8001",
		2: "localhost:8002",
		3: "localhost:8003",
	}
)

func main() {
	opts := nodeOpts{}
	flags.Parse(&opts)

	cfg := config.Config{
		NodeID:       opts.NodeID,
		ClusterID:    clusterID,
		ElectionRTT:  10,
		HeartbeatRTT: 1,
		CheckQuorum:  true,
		// TODO: set this much higher when I understand the perf implications of snapshotting
		SnapshotEntries:    100,
		CompactionOverhead: 10,
	}

	nodeAddr := initialClusterMembers[opts.NodeID]
	datadir := filepath.Join("data", "basic-data", fmt.Sprintf("node-%d", opts.NodeID))

	nhcfg := config.NodeHostConfig{
		WALDir:         datadir,
		NodeHostDir:    datadir,
		RTTMillisecond: 200,
		RaftAddress:    nodeAddr,
	}

	fmt.Fprintf(os.Stdout, "node address: %s\n", nodeAddr)
	// change the log verbosity
	logger.GetLogger("raft").SetLevel(logger.ERROR)
	logger.GetLogger("rsm").SetLevel(logger.WARNING)
	logger.GetLogger("transport").SetLevel(logger.WARNING)
	logger.GetLogger("grpc").SetLevel(logger.WARNING)

	nh, err := dragonboat.NewNodeHost(nhcfg)
	if err != nil {
		panic(err)
	}

	nh.StartCluster(initialClusterMembers, opts.Join, NewExampleStateMachine, cfg)

	ch := make(chan string, 16)
	consoleStopper := syncutil.NewStopper()
	raftStopper := syncutil.NewStopper()

	// Reads STDIN & pipes messages to the worker run by raftStopper
	consoleStopper.RunWorker(func() {
		reader := bufio.NewReader(os.Stdin)
		for {
			s, err := reader.ReadString('\n')
			if err != nil {
				close(ch)
				return
			}
			if s == "exit\n" {
				raftStopper.Stop()
				nh.Stop()
				return
			}
			ch <- s
		}
	})

	raftStopper.RunWorker(func() {
		cs := nh.GetNoOPSession(clusterID)
		for {
			select {
			case v, ok := <-ch:
				if !ok {
					return
				}
				// remove the \n char
				msg := strings.Replace(v, "\n", "", 1)
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				_, err := nh.SyncPropose(ctx, cs, []byte(msg))
				cancel()

				if err != nil {
					fmt.Fprintf(os.Stderr, "SyncPropose returned error %v\n", err)
				}
			case <-raftStopper.ShouldStop():
				return
			}
		}
	})

	fmt.Println("RUNNING RAFT SERVER")
	raftStopper.Wait()
}
