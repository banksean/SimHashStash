package simhashstash

import (
	"fmt"
	"github.com/mfonda/simhash"
	"github.com/petar/GoLLRB/llrb"
)

type Stash struct {
	// TODO: provide options for < 64 shards, with k/n windowing.
	// This memory usage might be prohibitive.
	tree [64]*llrb.LLRB
}

func NewStash() *Stash {
	tree := [64]*llrb.LLRB{}
	for i := 0; i < 64; i++ {
		tree[i] = llrb.New()
	}
	return &Stash{tree}
}

// Add adds k to the list of keys associated with v's simhash matches.
func (s *Stash) Add(k, v []byte) {
	// TODO: parameterize the FeatureSet implementation.
	h := simhash.Simhash(simhash.NewWordFeatureSet(v))
	// Store 64 rotations of the simhash, so lookups can use sorted
	// order to make search O(logn) using left leaning red-black trees.
	for i := 0; i < 64; i++ {
		// Store the original *key*, not the raw value bytes, in each node.
		var n node
		nk := node{h, nil}
		nn := s.tree[i].Get(nk)
		if nn == nil {
			n = node{h, nil}
		} else {
			n = nn.(node)
		}
		n.val = append(n.val, k)
		s.tree[i].ReplaceOrInsert(n)
		h = leftRot(h, 1)
	}
}

// Query returns any matches that are within thresh Hamming distance of in's simhash.
func (s *Stash) Query(in []byte, thresh uint8) [][]byte {
	h := simhash.Simhash(simhash.NewWordFeatureSet(in))
	seen := map[string]interface{}{}

	for i := 0; i < 64; i++ {
		pivot := node{key: h, val: [][]byte{}}
		for _, tree := range s.tree {
			tree.AscendGreaterOrEqual(pivot, func(i llrb.Item) bool {
				if simhash.Compare(pivot.key, i.(node).key) > thresh {
					return false
				}
				for _, v := range i.(node).val {
					seen[string(v)] = struct{}{}
				}
				return true
			})
			tree.DescendLessOrEqual(pivot, func(i llrb.Item) bool {
				if simhash.Compare(pivot.key, i.(node).key) > thresh {
					return false
				}
				for _, v := range i.(node).val {
					seen[string(v)] = struct{}{}
				}
				return true
			})
		}

		fmt.Printf("Adding %v from shard %d\n", seen, i)
		h = leftRot(h, 1)
	}

	ret := [][]byte{}
	// De-dupe keys we've pulled from multiple shards.
	for k, v := range seen {
		fmt.Printf("seen %d/%d %v = %v\n", len(ret), len(seen), k, v)
		ret = append(ret, []byte(k))
	}

	return ret
}

// Implements llrb.Item
type node struct {
	key uint64
	// val is a slice of byte slices because multiple values can
	// generate the same simhash, and we need to handle those collisions.
	val [][]byte
}

func (a node) Less(b llrb.Item) bool {
	return a.key < b.(node).key
}

func (a node) String() string {
	return fmt.Sprintf("%64b: %v", a.key, a.val)
}

// From https://github.com/golang/go/issues/18616#issuecomment-272598771
func leftRot(x uint64, k uint) uint64 {
	k &= 63
	return (x << k) | (x >> (64 - k))
}
