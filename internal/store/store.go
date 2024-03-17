package store

import "sync"

type Entry struct {
	value string
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

func (s *Store) Set(key, value string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.data[key] = Entry{value: value}
}

func (s *Store) Get(key string) string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	entry := s.data[key]

	return entry.value
}
