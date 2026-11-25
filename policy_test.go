package memo

import "testing"

func TestEvictionPolicies(t *testing.T) {
	tests := []struct {
		policy  Policy
		evicted int
	}{
		{LRU, 2},  // 1 was read after 2 was written
		{FIFO, 1}, // reads do not matter
		{LFU, 2},  // 1 was read, so 2 is used least
	}
	for _, tt := range tests {
		t.Run(tt.policy.String(), func(t *testing.T) {
			c := mustNew(t, Config[int, string]{Capacity: 2, Policy: tt.policy})
			c.Set(1, "a")
			c.Set(2, "b")
			c.Get(t.Context(), 1)
			c.Set(3, "c")
			if c.Len() != 2 || c.Has(tt.evicted) || !c.Has(3) {
				t.Fatalf("after eviction: len=%d has(%d)=%t", c.Len(), tt.evicted, c.Has(tt.evicted))
			}
		})
	}
	if Policy(9).String() != "Policy(unknown)" {
		t.Error("unknown policy String mismatch")
	}
}

func TestOverwriteDoesNotEvict(t *testing.T) {
	c := mustNew(t, Config[int, int]{Capacity: 2})
	c.Set(1, 1)
	c.Set(2, 2)
	c.Set(1, 10)
	if v, _ := c.Peek(1); v != 10 || !c.Has(2) {
		t.Fatal("overwriting an existing key evicted another entry")
	}
}
