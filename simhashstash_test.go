package simhashstash

import (
	"testing"
)

func TestNewStash(t *testing.T) {
	s := NewStash()
	if s == nil {
		t.Errorf("s was nil")
	}
}

func TestAdd(t *testing.T) {
	s := NewStash()
	s.Add([]byte("key1"), []byte("some text"))
	s.Add([]byte("key2"), []byte("some text"))

	res := s.Query([]byte("some text"), 64)
	if len(res) != 2 {
		t.Errorf("wrong number of results: %d, %v", len(res), res)
	}
}
