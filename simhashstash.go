package simhashstash

import (
	"fmt"
	"github.com/mfonda/simhash"

	// TODO: replace this lib with something maintained and with fewer
	// licensing issues.
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
		var n Node
		nk := Node{h, nil}
		nn := s.tree[i].Get(nk)
		if nn == nil {
			n = Node{h, nil}
		} else {
			n = nn.(Node)
		}
		n.Val = append(n.Val, k)
		s.tree[i].ReplaceOrInsert(n)
		h = leftRot(h, 1)
	}
}

// Query returns any matches that are within thresh Hamming distance of in's simhash.
func (s *Stash) Query(in []byte, thresh uint8) [][]byte {
	h := simhash.Simhash(simhash.NewWordFeatureSet(in))
	seen := map[string]interface{}{}

	pivot := Node{Key: h, Val: [][]byte{}}
	for _, tree := range s.tree {
		tree.AscendGreaterOrEqual(pivot, func(i llrb.Item) bool {
			if simhash.Compare(pivot.Key, i.(Node).Key) > thresh {
				return false
			}
			for _, v := range i.(Node).Val {
				seen[string(v)] = struct{}{}
			}
			return true
		})
		tree.DescendLessOrEqual(pivot, func(i llrb.Item) bool {
			if simhash.Compare(pivot.Key, i.(Node).Key) > thresh {
				return false
			}
			for _, v := range i.(Node).Val {
				seen[string(v)] = struct{}{}
			}
			return true
		})
	}

	ret := [][]byte{}
	// De-dupe keys we've pulled from multiple shards.
	for k := range seen {
		ret = append(ret, []byte(k))
	}

	return ret
}

// Implements llrb.Item
type Node struct {
	Key uint64
	// val is a slice of byte slices because multiple values can
	// generate the same simhash, and we need to handle those collisions.
	Val [][]byte
}

func (a Node) Less(b llrb.Item) bool {
	return a.Key < b.(Node).Key
}

func (a *Node) String() string {
	return fmt.Sprintf("%64b: %v", a.Key, a.Val)
}

// From https://github.com/golang/go/issues/18616#issuecomment-272598771
func leftRot(x uint64, k uint) uint64 {
	k &= 63
	return (x << k) | (x >> (64 - k))
}
