package main

import (
	"context"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"time"

	"github.com/rs/xid"

	"github.com/yammine/galvin"

	"github.com/yammine/galvin/storage"

	"github.com/tidwall/buntdb"

	"github.com/jessevdk/go-flags"
	"github.com/yammine/galvin/raft"
	"github.com/yammine/galvin/scheduler"

	"github.com/gin-gonic/gin"
	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/logger"
	"github.com/yammine/galvin/sequencer"
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

type req struct {
	Body []byte `form:"body" json:"body"`
}

func createRouter(seq *sequencer.Writer, nh *dragonboat.NodeHost) *gin.Engine {
	r := gin.Default()
	r.POST("/submit", func(c *gin.Context) {
		b := &req{}
		c.BindJSON(b)
		ref := xid.New()

		err := seq.SubmitTransaction(c, ref, b.Body)
		if err != nil {
			c.JSON(500, gin.H{"error": err})
			return
		}

		c.Status(200)
		return
	})
	r.GET("/get_batch", func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		q := c.Query("b")
		id, _ := strconv.Atoi(q)

		b, err := nh.SyncRead(ctx, 1, id)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(200, b)
		return
	})
	return r
}

func configure(opts nodeOpts) (config.Config, *dragonboat.NodeHost) {
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
	logger.GetLogger("rsm").SetLevel(logger.ERROR)
	logger.GetLogger("transport").SetLevel(logger.ERROR)
	logger.GetLogger("grpc").SetLevel(logger.ERROR)

	nh, err := dragonboat.NewNodeHost(nhcfg)
	if err != nil {
		panic(err)
	}
	return cfg, nh
}

func openDB() *buntdb.DB {
	err := os.MkdirAll("data/bunt/", fs.ModePerm)
	if err != nil {
		log.Fatal("could not create data dir for bunt", err)
	}

	db, err := buntdb.Open("data/bunt/data.db")
	if err != nil {
		log.Fatal("failed to open buntdb", err)
	}
	return db
}

func main() {
	opts := nodeOpts{}
	flags.Parse(&opts)

	cfg, nh := configure(opts)

	log.Println("Starting the node")
	err := nh.StartCluster(initialClusterMembers, opts.Join, raft.NewInMemory, cfg)
	if err != nil {
		log.Fatal("Failed to start the node: ", err)
	}

	db := openDB()
	ps := galvin.NewPubSub()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// The writer collects transaction requests and batch writes them to our raft log
	writer := sequencer.NewWriter(nh, ps)
	go writer.Run(ctx)

	// The scheduler will run transactions after they are committed to the log
	sch := scheduler.NewSequential(storage.WrapBuntDB(db), ps)
	go sch.Run(ctx)

	// The reader will fetch transactions that have been committed to the raft log and shuttle them to the scheduler
	reader := sequencer.NewReader(nh, sch)
	go reader.Run(ctx)

	// Create and run our HTTP server
	r := createRouter(writer, nh)
	go r.Run(fmt.Sprintf(":808%d", opts.NodeID))

	q := make(chan os.Signal)
	signal.Notify(q, os.Kill, os.Interrupt)

	<-q
	fmt.Println("Quitting now")
	nh.Stop()
}
