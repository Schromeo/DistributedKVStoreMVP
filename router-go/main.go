package main

import (
	"crypto/sha1"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"sync"
)

func hash32(s string) uint32 {
	sum := sha1.Sum([]byte(s))
	return binary.BigEndian.Uint32(sum[:4])
}

type Ring struct {
	mu     sync.RWMutex
	points []uint32          // sorted for binary search
	owner  map[uint32]string // int -> string
}

func NewRing() *Ring {
	return &Ring{owner: make(map[uint32]string)} // return the address of a new Ring
}

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

type RouteResp struct {
	Key       string `json:"key"`
	HashPoint uint32 `json:"hashPoint"`
	Leader    string `json:"leader"`
}

func main() {
	ring := NewRing()
	ring.AddNode("nodeA:8081", 16)
	ring.AddNode("nodeB:8082", 16)
	ring.AddNode("nodeC:8083", 16)

	http.HandleFunc("/route", func(w http.ResponseWriter, req *http.Request) {
		key := req.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "missing key", http.StatusBadRequest)
			return
		}
		leader, p := ring.GetNode(key)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(RouteResp{
			Key:       key,
			HashPoint: p,
			Leader:    leader,
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

	log.Println("router listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
