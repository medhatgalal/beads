package main

import (
	"container/heap"
	"crypto/sha256"
	"encoding/binary"
	"sort"
)

type boundedStoreEvidence struct {
	Seen      int64 `json:"seen"`
	Retained  int   `json:"retained"`
	Limit     int   `json:"limit"`
	Truncated bool  `json:"truncated"`
}

type reservoirEntry[T any] struct {
	key   string
	score uint64
	value T
}

// reservoirHeap is a max heap. The worst retained deterministic priority is
// always at index zero, allowing bounded O(log n) replacement.
type reservoirHeap[T any] []reservoirEntry[T]

func (h reservoirHeap[T]) Len() int { return len(h) }
func (h reservoirHeap[T]) Less(i, j int) bool {
	if h[i].score == h[j].score {
		return h[i].key > h[j].key
	}
	return h[i].score > h[j].score
}
func (h reservoirHeap[T]) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *reservoirHeap[T]) Push(value any) {
	*h = append(*h, value.(reservoirEntry[T]))
}
func (h *reservoirHeap[T]) Pop() any {
	old := *h
	value := old[len(old)-1]
	*h = old[:len(old)-1]
	return value
}

type deterministicReservoir[T any] struct {
	limit int
	seen  int64
	heap  reservoirHeap[T]
}

func newDeterministicReservoir[T any](limit int) *deterministicReservoir[T] {
	if limit < 0 {
		limit = 0
	}
	return &deterministicReservoir[T]{limit: limit}
}

func (r *deterministicReservoir[T]) add(key string, value T) (retained bool, evicted string) {
	r.seen++
	if r.limit == 0 {
		return false, ""
	}
	entry := reservoirEntry[T]{key: key, score: deterministicPriority(key), value: value}
	if len(r.heap) < r.limit {
		heap.Push(&r.heap, entry)
		return true, ""
	}
	worst := r.heap[0]
	if entry.score > worst.score || (entry.score == worst.score && entry.key >= worst.key) {
		return false, ""
	}
	heap.Pop(&r.heap)
	heap.Push(&r.heap, entry)
	return true, worst.key
}

func (r *deterministicReservoir[T]) entries() []reservoirEntry[T] {
	result := append([]reservoirEntry[T](nil), r.heap...)
	sort.Slice(result, func(i, j int) bool {
		if result[i].score == result[j].score {
			return result[i].key < result[j].key
		}
		return result[i].score < result[j].score
	})
	return result
}

func (r *deterministicReservoir[T]) evidence() boundedStoreEvidence {
	return boundedStoreEvidence{
		Seen: r.seen, Retained: len(r.heap), Limit: r.limit,
		Truncated: r.seen > int64(len(r.heap)),
	}
}

func deterministicPriority(key string) uint64 {
	sum := sha256.Sum256([]byte(key))
	return binary.BigEndian.Uint64(sum[:8])
}

type boundedIDProof struct {
	reservoir *deterministicReservoir[string]
	retained  map[string]struct{}
}

func newBoundedIDProof(limit int) *boundedIDProof {
	return &boundedIDProof{
		reservoir: newDeterministicReservoir[string](limit),
		retained:  make(map[string]struct{}, limit),
	}
}

func (p *boundedIDProof) observe(id string) bool {
	if _, exists := p.retained[id]; exists {
		return true
	}
	retained, evicted := p.reservoir.add(id, id)
	if retained {
		if evicted != "" {
			delete(p.retained, evicted)
		}
		p.retained[id] = struct{}{}
	}
	return false
}

func (p *boundedIDProof) evidence() boundedStoreEvidence {
	return p.reservoir.evidence()
}
