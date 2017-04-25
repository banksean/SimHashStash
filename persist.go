package simhashstash

import (
	"encoding/gob"
	//"fmt"
	"io"

	// TODO: replace this lib with something maintained and with fewer
	// licensing issues.
	"github.com/petar/GoLLRB/llrb"
)

type PersistedTree struct {
	Nodes []Node
}

func (s *Stash) WriteTo(stream io.Writer) error {
	// Naive implementation: traverse the llrb's and write them out one by one.
	enc := gob.NewEncoder(stream)
	for _, tree := range s.tree {
		pt := &PersistedTree{}
		tree.AscendGreaterOrEqual(tree.Min(), func(i llrb.Item) bool {
			n, _ := i.(Node)
			pt.Nodes = append(pt.Nodes, n)
			return true // Ascend the entire list.
		})
		if err := enc.Encode(*pt); err != nil {
			return err
		}
	}
	return nil
}

func (s *Stash) ReadFrom(stream io.Reader) error {
	dec := gob.NewDecoder(stream)
	s.tree = [64]*llrb.LLRB{}
	for i := 0; i < 64; i++ {
		pt := PersistedTree{}
		err := dec.Decode(&pt)
		if err != nil {
			if err != io.EOF {
				return err
			}
			return nil
		}
		s.tree[i] = llrb.New()
		for _, n := range pt.Nodes {
			s.tree[i].ReplaceOrInsert(n)
		}
	}
	return nil
}
