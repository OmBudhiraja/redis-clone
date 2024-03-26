package store

import (
	"errors"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/internal/store/datatypes"
)

type Data interface {
	GetType() string
}

type Store struct {
	data  map[string]Data
	mutex *sync.RWMutex
}

func New() *Store {
	return &Store{
		data:  make(map[string]Data),
		mutex: &sync.RWMutex{},
	}
}

func (s *Store) Set(key, value string, expiry time.Time) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.data[key] = &datatypes.String{
		DataType: "string",
		Value:    value,
		Expiry:   expiry,
	}
}

func (s *Store) XAdd(streamKey, entryId string, entries []string) (string, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	stream, ok := s.data[streamKey].(*datatypes.Stream)

	if !ok {
		stream = &datatypes.Stream{
			DataType: "stream",
			Values:   make([]datatypes.Entry, 0),
		}
	}
	id, err := stream.AddEntry(entryId, entries)

	if err != nil {
		return "", err
	}

	s.data[streamKey] = stream
	return id, nil
}

func (s *Store) XRange(streamKey, startId, endId string) ([]datatypes.Entry, error) {

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	stream, ok := s.data[streamKey].(*datatypes.Stream)

	if !ok {
		return nil, errors.New("ERR no such stream key")
	}

	entries, err := stream.GetRange(startId, endId)

	return entries, err
}

func (s *Store) Get(key string) string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	e, ok := s.data[key]

	if !ok {
		return ""
	}

	entry, ok := e.(*datatypes.String)

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

func (s *Store) GetDataType(key string) string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	e, ok := s.data[key]

	if !ok {
		return "none"
	}

	return e.GetType()
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
