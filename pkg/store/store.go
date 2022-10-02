package store

import (
	"context"
	"fmt"
	"github.com/matrixorigin/talent-challenge/matrixbase/distributed/pkg/cfg"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"time"
)

// Store the store interface
type Store interface {
	// Set set key-value to store
	Set(key []byte, value []byte) error
	// Get returns the value from store
	Get(key []byte) ([]byte, error)
	// Delete remove the key from store
	Delete(key []byte) error
}

// NewStore create the raft store
func NewStore(cfg cfg.StoreCfg) (Store, error) {
	if cfg.Memory {
		return newMemoryStore()
	}
	proposeC := make(chan string)
	confChangeC := make(chan raftpb.ConfChange)
	stopc := make(chan struct{})
	storage := raft.NewMemoryStorage()

	db, err := NewPebbleDB("/wrk")
	if err != nil {
		return nil, err
	}
	store := raftStore{
		node:        nil,
		storage:     storage,
		proposeC:    proposeC,
		confChangeC: confChangeC,
		stopc:       stopc,
		db:          db,
		ticker:      time.NewTicker(100 * time.Millisecond),
	}
	go store.start()
	return &store, nil
}

type raftStore struct {
	id      int
	node    raft.Node
	storage raft.Storage

	proposeC    chan string            // proposed messages (k,v)
	confChangeC chan raftpb.ConfChange // proposed cluster config changes
	stopc       chan struct{}          // signals proposal channel closed

	ticker *time.Ticker
	db     *PebbleDB
}

func (r *raftStore) start() {
	c := &raft.Config{
		ID:                        0x20,
		ElectionTick:              10,
		HeartbeatTick:             1,
		Storage:                   r.storage,
		MaxSizePerMsg:             1024 * 1024,
		MaxInflightMsgs:           256,
		MaxUncommittedEntriesSize: 1 << 30,
	}
	r.node = raft.StartNode(c,
		[]raft.Peer{
			{ID: 0x1},
			{ID: 0x2},
			{ID: 0x3},
		})
}
func (r *raftStore) serveChannel() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// send proposals over raft
	go func() {
		confChangeCount := uint64(0)
		for r.proposeC != nil && r.confChangeC != nil {
			select {
			case prop, ok := <-r.proposeC:
				fmt.Printf("prop:[%v]", prop)
				if !ok {
					r.proposeC = nil
				} else {
					// blocks until accepted by raft state machine
					err := r.node.Propose(context.TODO(), []byte(prop))
					if err != nil {
						fmt.Printf("err: [%v]\n", err)
						continue
					}
				}
			case cc, ok := <-r.confChangeC:
				fmt.Printf("confChangeC:[%v]", cc)
				if !ok {
					r.confChangeC = nil
				} else {
					confChangeCount++
					cc.ID = confChangeCount
					err := r.node.ProposeConfChange(context.TODO(), cc)
					if err != nil {
						fmt.Printf("err: [%v]\n", err)
						continue
					}
				}
			}
		}
		// client closed channel; shutdown raft if not already
		close(r.stopc)
	}()

	go func() {
		// event loop on raft state machine updates
		for {
			select {
			case <-ticker.C:
				r.node.Tick()
			// store raft entries to wal, then publish over commit channel
			case rd := <-r.node.Ready():
				fmt.Printf("%+v", rd)
				r.node.Advance()
			case <-r.stopc:
				return
			}
		}
	}()
}
func (r *raftStore) Set(key []byte, value []byte) error {
	fmt.Println("set")
	r.proposeC <- fmt.Sprintf("set[%v][%v]", key, value)
	return r.db.Set(key, value)
}
func (r *raftStore) Get(key []byte) ([]byte, error) {
	fmt.Println("get")
	r.proposeC <- fmt.Sprintf("get[%v]", key)
	return r.db.Get(key)
}
func (r *raftStore) Delete(key []byte) error {
	fmt.Println("delete")
	r.proposeC <- fmt.Sprintf("delete[%v]", key)
	return r.db.Delete(key)
}
