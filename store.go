package memo

// nilIndex marks the end of a list.
const nilIndex int32 = -1

// node is one cache entry. Nodes live in one slice and refer to each other by
// index, so adding and evicting entries reuses memory instead of allocating a
// list element or entry per key.
type node[K comparable, V any] struct {
	key       K
	value     V
	expires   int64 // nanoseconds since the cache's epoch; 0 means never
	isMissing bool

	// LRU and FIFO: a doubly linked list ordered from next victim to newest.
	prev, next int32

	// LFU: the position in the heap, and the ordering keys.
	pos  int32
	freq uint32
	seq  uint64
}

// store holds the entries of one shard and their eviction order. It is not
// safe for concurrent use.
type store[K comparable, V any] struct {
	policy   Policy
	capacity int
	index    map[K]int32
	nodes    []node[K, V]
	free     []int32

	head, tail int32   // LRU and FIFO list
	heap       []int32 // LFU min-heap ordered by (freq, seq)
	seq        uint64
}

func newStore[K comparable, V any](policy Policy, capacity int) store[K, V] {
	return store[K, V]{
		policy:   policy,
		capacity: capacity,
		index:    make(map[K]int32, capacity),
		head:     nilIndex,
		tail:     nilIndex,
	}
}

func (s *store[K, V]) len() int { return len(s.index) }

// lookup returns the index of key's node.
func (s *store[K, V]) lookup(key K) (int32, bool) {
	i, ok := s.index[key]
	return i, ok
}

// put stores an entry for key, evicting entries first when the store is full.
// Overwriting a key counts as a use for LRU and LFU.
func (s *store[K, V]) put(key K, value V, expires int64, isMissing bool) {
	if i, ok := s.index[key]; ok {
		n := &s.nodes[i]
		n.value, n.expires, n.isMissing = value, expires, isMissing
		s.touch(i)
		return
	}
	for len(s.index) >= s.capacity {
		s.removeAt(s.victim())
	}
	var i int32
	if last := len(s.free) - 1; last >= 0 {
		i = s.free[last]
		s.free = s.free[:last]
	} else {
		s.nodes = append(s.nodes, node[K, V]{})
		i = int32(len(s.nodes) - 1)
	}
	s.nodes[i] = node[K, V]{key: key, value: value, expires: expires, isMissing: isMissing}
	s.index[key] = i
	if s.policy == LFU {
		s.seq++
		s.nodes[i].freq, s.nodes[i].seq = 1, s.seq
		s.heapPush(i)
	} else {
		s.pushBack(i)
	}
}

// touch records a use of node i.
func (s *store[K, V]) touch(i int32) {
	switch s.policy {
	case LRU:
		if s.tail != i {
			s.unlink(i)
			s.pushBack(i)
		}
	case LFU:
		s.seq++
		s.nodes[i].freq++
		s.nodes[i].seq = s.seq
		s.down(int(s.nodes[i].pos)) // the node only grew, so it can only sink
	}
}

// remove deletes key and reports whether it was present.
func (s *store[K, V]) remove(key K) bool {
	i, ok := s.index[key]
	if ok {
		s.removeAt(i)
	}
	return ok
}

func (s *store[K, V]) removeAt(i int32) {
	if s.policy == LFU {
		s.heapRemove(int(s.nodes[i].pos))
	} else {
		s.unlink(i)
	}
	delete(s.index, s.nodes[i].key)
	s.nodes[i] = node[K, V]{} // drop references so the GC can reclaim them
	s.free = append(s.free, i)
}

// victim returns the node to evict next. The store must not be empty.
func (s *store[K, V]) victim() int32 {
	if s.policy == LFU {
		return s.heap[0]
	}
	return s.head
}

func (s *store[K, V]) reset() {
	*s = newStore[K, V](s.policy, s.capacity)
}

func (s *store[K, V]) pushBack(i int32) {
	n := &s.nodes[i]
	n.prev, n.next = s.tail, nilIndex
	if s.tail != nilIndex {
		s.nodes[s.tail].next = i
	} else {
		s.head = i
	}
	s.tail = i
}

func (s *store[K, V]) unlink(i int32) {
	n := &s.nodes[i]
	if n.prev != nilIndex {
		s.nodes[n.prev].next = n.next
	} else {
		s.head = n.next
	}
	if n.next != nilIndex {
		s.nodes[n.next].prev = n.prev
	} else {
		s.tail = n.prev
	}
}

func (s *store[K, V]) less(a, b int32) bool {
	na, nb := &s.nodes[a], &s.nodes[b]
	if na.freq != nb.freq {
		return na.freq < nb.freq
	}
	return na.seq < nb.seq
}

func (s *store[K, V]) swap(i, j int) {
	s.heap[i], s.heap[j] = s.heap[j], s.heap[i]
	s.nodes[s.heap[i]].pos = int32(i)
	s.nodes[s.heap[j]].pos = int32(j)
}

func (s *store[K, V]) heapPush(i int32) {
	s.heap = append(s.heap, i)
	s.nodes[i].pos = int32(len(s.heap) - 1)
	s.up(len(s.heap) - 1)
}

func (s *store[K, V]) heapRemove(p int) {
	last := len(s.heap) - 1
	if p != last {
		s.swap(p, last)
	}
	s.heap = s.heap[:last]
	if p != last {
		s.down(p)
		s.up(p)
	}
}

func (s *store[K, V]) up(j int) {
	for j > 0 {
		parent := (j - 1) / 2
		if !s.less(s.heap[j], s.heap[parent]) {
			return
		}
		s.swap(j, parent)
		j = parent
	}
}

func (s *store[K, V]) down(i int) {
	n := len(s.heap)
	for {
		left := 2*i + 1
		if left >= n {
			return
		}
		child := left
		if right := left + 1; right < n && s.less(s.heap[right], s.heap[left]) {
			child = right
		}
		if !s.less(s.heap[child], s.heap[i]) {
			return
		}
		s.swap(i, child)
		i = child
	}
}
