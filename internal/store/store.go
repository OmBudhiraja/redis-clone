package store

import (
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/internal/rdb"
)

type Store struct {
	data  map[string]rdb.Entry
	mutex *sync.RWMutex
}

func New(file *rdb.RDBFile) *Store {
	return &Store{
		data:  file.Items,
		mutex: &sync.RWMutex{},
	}
}

func (s *Store) Set(key, value string, expiry time.Time) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.data[key] = rdb.Entry{
		Value:  value,
		Expiry: expiry,
	}
}

func (s *Store) Get(key string) string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	entry, ok := s.data[key]

	if !ok {
		return ""
	}

	if entry.Expiry.IsZero() {
		return entry.Value
	}

	if time.Now().After(entry.Expiry) {
		delete(s.data, key)
		return ""
	}

	return entry.Value
}

func (s *Store) GetKeysWithPattern(pattern string) []string {

	// TODO: Implement other patters
	if pattern != "*" {
		return []string{}
	}

	keys := []string{}

	for key := range s.data {
		keys = append(keys, key)
	}

	return keys
}
