package store

import (
	"sync"
	"time"
)

type Entry struct {
	value  string
	expiry time.Time
}

type Store struct {
	data  map[string]Entry
	mutex sync.RWMutex
}

func New() *Store {
	return &Store{
		data:  make(map[string]Entry),
		mutex: sync.RWMutex{},
	}
}

func (s *Store) Set(key, value string, expiry time.Time) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.data[key] = Entry{
		value:  value,
		expiry: expiry,
	}
}

func (s *Store) Get(key string) string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	entry, ok := s.data[key]

	if !ok {
		return ""
	}

	if entry.expiry.IsZero() {
		return entry.value
	}

	if time.Now().After(entry.expiry) {
		delete(s.data, key)
		return ""
	}

	return entry.value
}
