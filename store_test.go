package memo

import (
	"math/rand/v2"
	"slices"
	"testing"
)

// model is a deliberately simple reference implementation of the eviction
// policies: a slice of keys ordered from next victim to newest, plus use
// counts and access order for LFU.
type model struct {
	policy   Policy
	capacity int
	order    []int          // LRU and FIFO: next victim first
	freq     map[int]int    // LFU
	lastUse  map[int]uint64 // LFU tie-break: older access is evicted first
	clock    uint64
}

func (m *model) has(k int) bool {
	_, ok := m.freq[k]
	return ok
}

func (m *model) use(k int) {
	m.clock++
	m.freq[k]++
	m.lastUse[k] = m.clock
	if m.policy == LRU {
		i := slices.Index(m.order, k)
		m.order = append(slices.Delete(m.order, i, i+1), k)
	}
}

func (m *model) victim() int {
	if m.policy != LFU {
		return m.order[0]
	}
	best := -1
	for k := range m.freq {
		if best == -1 || m.freq[k] < m.freq[best] || (m.freq[k] == m.freq[best] && m.lastUse[k] < m.lastUse[best]) {
			best = k
		}
	}
	return best
}

func (m *model) remove(k int) {
	delete(m.freq, k)
	delete(m.lastUse, k)
	if i := slices.Index(m.order, k); i >= 0 {
		m.order = slices.Delete(m.order, i, i+1)
	}
}

func (m *model) put(k int) {
	if m.has(k) {
		if m.policy != FIFO {
			m.use(k)
		}
		return
	}
	for len(m.freq) >= m.capacity {
		m.remove(m.victim())
	}
	m.clock++
	m.freq[k], m.lastUse[k] = 1, m.clock
	m.order = append(m.order, k)
}

// TestStoreMatchesModel runs random operations against the store and the
// reference model and compares their contents after every step.
func TestStoreMatchesModel(t *testing.T) {
	for _, policy := range []Policy{LRU, LFU, FIFO} {
		t.Run(policy.String(), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(1, uint64(policy)))
			s := newStore[int, int](policy, 8)
			m := &model{policy: policy, capacity: 8, freq: map[int]int{}, lastUse: map[int]uint64{}}
			for step := range 20_000 {
				k := rng.IntN(20)
				switch op := rng.IntN(10); {
				case op < 5:
					s.put(k, k*10, 0, false)
					m.put(k)
				case op < 8:
					if i, ok := s.lookup(k); ok {
						if s.nodes[i].value != k*10 {
							t.Fatalf("step %d: value of %d = %d", step, k, s.nodes[i].value)
						}
						s.touch(i)
					}
					if m.has(k) && m.policy != FIFO {
						m.use(k)
					}
				default:
					if s.remove(k) != m.has(k) {
						t.Fatalf("step %d: remove(%d) disagrees with the model", step, k)
					}
					m.remove(k)
				}
				if s.len() != len(m.freq) {
					t.Fatalf("step %d: len = %d, model %d", step, s.len(), len(m.freq))
				}
				for k := range m.freq {
					if _, ok := s.lookup(k); !ok {
						t.Fatalf("step %d: key %d missing from the store", step, k)
					}
				}
				if s.len() > 0 && s.nodes[s.victim()].key != m.victim() {
					t.Fatalf("step %d: victim = %d, model %d", step, s.nodes[s.victim()].key, m.victim())
				}
			}
		})
	}
}

func TestStoreReusesNodesAndDropsReferences(t *testing.T) {
	s := newStore[int, *int](LRU, 2)
	v := new(42)
	s.put(1, v, 0, false)
	s.put(2, v, 0, false)
	s.put(3, v, 0, false) // evicts 1
	if len(s.nodes) != 2 {
		t.Fatalf("nodes = %d, want 2: an evicted node must be reused", len(s.nodes))
	}
	s.remove(2)
	for _, i := range s.free {
		if s.nodes[i].value != nil {
			t.Fatal("a free node still references its value")
		}
	}
}
