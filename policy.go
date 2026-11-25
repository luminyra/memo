package memo

import (
	"container/heap"
	"container/list"
)

// Policy selects which entry to evict when the cache is full.
type Policy int

const (
	// LRU evicts the least recently used entry.
	LRU Policy = iota
	// LFU evicts the least frequently used entry; ties go to the oldest access.
	LFU
	// FIFO evicts the oldest inserted entry, ignoring reads.
	FIFO
)

// String returns the policy name.
func (p Policy) String() string {
	switch p {
	case LRU:
		return "LRU"
	case LFU:
		return "LFU"
	case FIFO:
		return "FIFO"
	default:
		return "Policy(unknown)"
	}
}

func (p Policy) isValid() bool { return p >= LRU && p <= FIFO }

// evictor tracks key order for one policy. It is not safe for concurrent use.
type evictor[K comparable] interface {
	add(key K)
	touch(key K)
	remove(key K)
	victim() (K, bool)
}

func newEvictor[K comparable](p Policy) evictor[K] {
	switch p {
	case LFU:
		return &lfu[K]{index: make(map[K]*lfuItem[K])}
	case FIFO:
		return &listEvictor[K]{order: list.New(), index: make(map[K]*list.Element), isLRU: false}
	default:
		return &listEvictor[K]{order: list.New(), index: make(map[K]*list.Element), isLRU: true}
	}
}

// listEvictor implements LRU (isLRU) and FIFO. The front is the next
// victim.
type listEvictor[K comparable] struct {
	order *list.List
	index map[K]*list.Element
	isLRU bool
}

func (l *listEvictor[K]) add(key K) {
	if e, ok := l.index[key]; ok {
		if l.isLRU {
			l.order.MoveToBack(e)
		}
		return
	}
	l.index[key] = l.order.PushBack(key)
}

func (l *listEvictor[K]) touch(key K) {
	if e, ok := l.index[key]; ok && l.isLRU {
		l.order.MoveToBack(e)
	}
}

func (l *listEvictor[K]) remove(key K) {
	if e, ok := l.index[key]; ok {
		l.order.Remove(e)
		delete(l.index, key)
	}
}

func (l *listEvictor[K]) victim() (K, bool) {
	front := l.order.Front()
	if front == nil {
		var zero K
		return zero, false
	}
	return front.Value.(K), true
}

// lfu is a min-heap ordered by frequency, then by last access sequence.
type lfu[K comparable] struct {
	items []*lfuItem[K]
	index map[K]*lfuItem[K]
	seq   uint64
}

type lfuItem[K comparable] struct {
	key  K
	freq uint64
	seq  uint64
	pos  int
}

func (h *lfu[K]) Len() int { return len(h.items) }
func (h *lfu[K]) Less(i, j int) bool {
	a, b := h.items[i], h.items[j]
	if a.freq != b.freq {
		return a.freq < b.freq
	}
	return a.seq < b.seq
}
func (h *lfu[K]) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.items[i].pos, h.items[j].pos = i, j
}
func (h *lfu[K]) Push(x any) {
	item := x.(*lfuItem[K])
	item.pos = len(h.items)
	h.items = append(h.items, item)
}
func (h *lfu[K]) Pop() any {
	last := h.items[len(h.items)-1]
	h.items[len(h.items)-1] = nil
	h.items = h.items[:len(h.items)-1]
	return last
}

func (h *lfu[K]) add(key K) {
	if _, ok := h.index[key]; ok {
		h.touch(key)
		return
	}
	h.seq++
	item := &lfuItem[K]{key: key, freq: 1, seq: h.seq}
	h.index[key] = item
	heap.Push(h, item)
}

func (h *lfu[K]) touch(key K) {
	item, ok := h.index[key]
	if !ok {
		return
	}
	h.seq++
	item.freq++
	item.seq = h.seq
	heap.Fix(h, item.pos)
}

func (h *lfu[K]) remove(key K) {
	item, ok := h.index[key]
	if !ok {
		return
	}
	heap.Remove(h, item.pos)
	delete(h.index, key)
}

func (h *lfu[K]) victim() (K, bool) {
	if len(h.items) == 0 {
		var zero K
		return zero, false
	}
	return h.items[0].key, true
}
