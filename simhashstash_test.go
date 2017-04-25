package simhashstash

import (
	"bytes"
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

func TestWriteTo(t *testing.T) {
	var b bytes.Buffer
	s := NewStash()
	s.Add([]byte("key"), []byte("value"))
	err := s.WriteTo(&b)
	if err != nil {
		t.Errorf("error writing stash: %v", err)
	}
}

func TestReadFrom(t *testing.T) {
	var b bytes.Buffer
	old := NewStash()
	old.Add([]byte("key"), []byte("value"))
	res := old.Query([]byte("value"), 0)
	if len(res) != 1 {
		t.Errorf("wrong number of values for key before read: %v", len(res))
	}
	err := old.WriteTo(&b)
	if err != nil {
		t.Errorf("error writing stash: %v", err)
	}
	newS := NewStash()
	err = newS.ReadFrom(&b)
	if err != nil {
		t.Errorf("error reading stash: %v", err)
	}
	res = newS.Query([]byte("value"), 0)
	if len(res) != 1 {
		t.Errorf("wrong number of values for key after read: %v", len(res))
	}
}
