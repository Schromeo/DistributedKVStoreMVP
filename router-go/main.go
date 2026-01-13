package main

import (
	"crypto/sha1"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"
)

func hash32(s string) uint32 {
	sum := sha1.Sum([]byte(s))
	return binary.BigEndian.Uint32(sum[:4])
}

/************** KVStore Interface **************/
type KVStore interface {
	Put(k, v string) error
	Get(k string) (string, bool, error)
	Close()
}

/************** Consistent Hash Ring **************/
type Ring struct {
	mu     sync.RWMutex
	points []uint32
	owner  map[uint32]string // point -> nodeID
}

func NewRing() *Ring { return &Ring{owner: make(map[uint32]string)} }

func (r *Ring) AddNode(nodeID string, vnodes int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := 0; i < vnodes; i++ {
		p := hash32(fmt.Sprintf("%s#%d", nodeID, i))
		r.points = append(r.points, p)
		r.owner[p] = nodeID
	}
	sort.Slice(r.points, func(i, j int) bool { return r.points[i] < r.points[j] })
}

func (r *Ring) GetNode(key string) (nodeID string, point uint32) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.points) == 0 {
		return "", 0
	}
	k := hash32(key)
	i := sort.Search(len(r.points), func(i int) bool { return r.points[i] >= k })
	if i == len(r.points) {
		i = 0 // wrap
	}
	p := r.points[i]
	return r.owner[p], p
}

/************** Multi-Raft (Mock) **************/
type LogEntry struct {
	Index int
	Key   string
	Value string
}

type RaftGroup struct {
	mu          sync.Mutex
	ShardID     int
	Leader      string
	Peers       []string
	CommitIndex int
	Log         []LogEntry
	ApplyCh     chan LogEntry
}

func NewRaftGroup(shardID int, leader string, peers []string) *RaftGroup {
	return &RaftGroup{
		ShardID: shardID,
		Leader:  leader,
		Peers:   peers,
		ApplyCh: make(chan LogEntry, 256),
	}
}

// Mock: assume majority acks after tiny delay; focus on orchestration + apply pipeline.
func (g *RaftGroup) Propose(key, value string) int {
	g.mu.Lock()
	defer g.mu.Unlock()

	idx := len(g.Log) + 1
	ent := LogEntry{Index: idx, Key: key, Value: value}
	g.Log = append(g.Log, ent)

	log.Printf("[RAFT] shard=%d leader=%s propose idx=%d key=%s", g.ShardID, g.Leader, idx, key)

	// mock commit async
	go func(e LogEntry) {
		time.Sleep(15 * time.Millisecond) // simulate replication/majority ack
		g.mu.Lock()
		if e.Index > g.CommitIndex {
			g.CommitIndex = e.Index
		}
		g.mu.Unlock()
		log.Printf("[RAFT] shard=%d committed idx=%d", g.ShardID, e.Index)
		g.ApplyCh <- e
	}(ent)

	return idx
}

/************** MemKV (still useful for fallback/debug) **************/
type MemKV struct {
	mu sync.RWMutex
	m  map[string]string
}

func NewMemKV() *MemKV { return &MemKV{m: make(map[string]string)} }

func (kv *MemKV) Put(k, v string) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.m[k] = v
	return nil
}
func (kv *MemKV) Get(k string) (string, bool, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	v, ok := kv.m[k]
	return v, ok, nil
}
func (kv *MemKV) Close() {}

/************** Node (hosts multiple raft groups) **************/
type Node struct {
	NodeID string
	Groups map[int]*RaftGroup // shardID -> group
	Store  KVStore
}

type Cluster struct {
	Ring      *Ring
	NumShards int
	Nodes     map[string]*Node // nodeID -> node
}

// deterministic shard from key (shard != ring; ring decides leader/owner)
func (c *Cluster) shardOf(key string) int {
	return int(hash32(key) % uint32(c.NumShards))
}

func (c *Cluster) routeLeader(key string) (leader string, point uint32) {
	return c.Ring.GetNode(key)
}

func main() {
	// ---- Build ring ----
	ring := NewRing()
	ring.AddNode("nodeA:8081", 16)
	ring.AddNode("nodeB:8082", 16)
	ring.AddNode("nodeC:8083", 16)

	cluster := &Cluster{
		Ring:      ring,
		NumShards: 3,
		Nodes:     make(map[string]*Node),
	}

	// create nodes (Store uses C++ engine via cgo: CppStore)
	for _, id := range []string{"nodeA:8081", "nodeB:8082", "nodeC:8083"} {
		cluster.Nodes[id] = &Node{
			NodeID: id,
			Groups: make(map[int]*RaftGroup),
			Store:  NewCppStore(), // <--- C++ store
		}
	}

	// assign shard leaders (Multi-Raft: leaders spread)
	shardLeader := map[int]string{
		0: "nodeA:8081",
		1: "nodeB:8082",
		2: "nodeC:8083",
	}
	peers := []string{"nodeA:8081", "nodeB:8082", "nodeC:8083"}

	// create raft group per shard, hosted on every node (follower semantics omitted in MVP)
	for shard := 0; shard < cluster.NumShards; shard++ {
		leader := shardLeader[shard]
		for _, n := range cluster.Nodes {
			n.Groups[shard] = NewRaftGroup(shard, leader, peers)
		}
	}

	// start appliers on every node:
	// when group applies on leader, apply to leader's local store (C++ engine here)
	for _, n := range cluster.Nodes {
		for _, g := range n.Groups {
			go func(node *Node, group *RaftGroup) {
				for e := range group.ApplyCh {
					// only leader applies in this MVP
					if node.NodeID != group.Leader {
						continue
					}
					_ = node.Store.Put(e.Key, e.Value)
					log.Printf("[APPLY] shard=%d leader=%s apply idx=%d -> store[%s]=%s",
						group.ShardID, group.Leader, e.Index, e.Key, e.Value)
				}
			}(n, g)
		}
	}

	// ---- HTTP Handlers ----
	type RouteResp struct {
		Key       string `json:"key"`
		HashPoint uint32 `json:"hashPoint"`
		Leader    string `json:"leader"`
		ShardID   int    `json:"shardId"`
	}

	http.HandleFunc("/route", func(w http.ResponseWriter, req *http.Request) {
		key := req.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "missing key", http.StatusBadRequest)
			return
		}
		leader, p := cluster.routeLeader(key)
		shardID := cluster.shardOf(key)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(RouteResp{Key: key, HashPoint: p, Leader: leader, ShardID: shardID})
	})

	http.HandleFunc("/put", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "POST required", http.StatusMethodNotAllowed)
			return
		}
		key := req.URL.Query().Get("key")
		val := req.URL.Query().Get("value")
		if key == "" {
			http.Error(w, "missing key", http.StatusBadRequest)
			return
		}

		leader, _ := cluster.routeLeader(key)
		shardID := cluster.shardOf(key)
		log.Printf("[ROUTE] key=%s shard=%d -> leader=%s", key, shardID, leader)

		leaderNode := cluster.Nodes[leader]
		if leaderNode == nil {
			http.Error(w, "leader not found", http.StatusInternalServerError)
			return
		}
		group := leaderNode.Groups[shardID]
		if group == nil {
			http.Error(w, "raft group not found", http.StatusInternalServerError)
			return
		}

		idx := group.Propose(key, val)

		// for demo: wait until committed (small timeout) so GET after PUT is consistent
		deadline := time.Now().Add(500 * time.Millisecond)
		for time.Now().Before(deadline) {
			group.mu.Lock()
			committed := group.CommitIndex >= idx
			group.mu.Unlock()
			if committed {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":     true,
			"key":    key,
			"value":  val,
			"shard":  shardID,
			"leader": leader,
			"index":  idx,
		})
	})

	http.HandleFunc("/get", func(w http.ResponseWriter, req *http.Request) {
		key := req.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "missing key", http.StatusBadRequest)
			return
		}
		leader, _ := cluster.routeLeader(key)
		shardID := cluster.shardOf(key)

		leaderNode := cluster.Nodes[leader]
		if leaderNode == nil {
			http.Error(w, "leader not found", http.StatusInternalServerError)
			return
		}
		v, ok, _ := leaderNode.Store.Get(key)
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"key":    key,
			"value":  v,
			"shard":  shardID,
			"leader": leader,
		})
	})

	http.HandleFunc("/ring", func(w http.ResponseWriter, req *http.Request) {
		ring.mu.RLock()
		defer ring.mu.RUnlock()
		type P struct {
			Point uint32 `json:"point"`
			Node  string `json:"node"`
		}
		out := make([]P, 0, len(ring.points))
		for _, pt := range ring.points {
			out = append(out, P{Point: pt, Node: ring.owner[pt]})
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	})

	http.HandleFunc("/debug/shardleaders", func(w http.ResponseWriter, req *http.Request) {
		out := make(map[string]string)
		for shard := 0; shard < cluster.NumShards; shard++ {
			out[strconv.Itoa(shard)] = shardLeader[shard]
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	})

	log.Println("router+multiraft+cgo(C++) listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
