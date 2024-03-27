package store

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
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
			DataType:    "stream",
			Values:      make([]datatypes.Entry, 0),
			Subscribers: make(map[string]chan string),
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

func (s *Store) XRead(streamKey, entryId string, count, block int, ctx context.Context, ctxCancel context.CancelFunc) ([]datatypes.Entry, error) {

	stream, ok := s.data[streamKey].(*datatypes.Stream)

	if !ok {
		return nil, errors.New("ERR no such stream key")
	}
	randomSubscriberKey := randomString()
	stream.Subscribers[randomSubscriberKey] = make(chan string)

	if entryId == "$" {
		if len(stream.Values) == 0 {
			entryId = "0-0"
		} else {
			lastEntry := stream.Values[len(stream.Values)-1]
			entryId = lastEntry.Id
		}
	}

	// block arg not passed
	if block == -1 {
		return stream.ReadEntry(entryId, count)
	}

	resultEntries := []datatypes.Entry{}

	// close the subscriber channel when the context is done
	go func() {
		<-ctx.Done()

		if _, ok := stream.Subscribers[randomSubscriberKey]; ok {
			stream.Subscribers[randomSubscriberKey] <- "done"
		}
	}()

	for {
		entries, err := stream.ReadEntry(entryId, count)

		if err != nil {
			return nil, err
		}

		if len(entries) > 0 {
			entryId = entries[len(entries)-1].Id
			resultEntries = append(resultEntries, entries...)
		}

		if len(entries) == count {
			break
		}

		count -= len(entries)

		res := <-stream.Subscribers[randomSubscriberKey]

		if res == "done" {
			break
		}
	}

	close(stream.Subscribers[randomSubscriberKey])
	delete(stream.Subscribers, randomSubscriberKey)

	ctxCancel()

	return resultEntries, nil

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

func randomString() string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	// Convert charset string to byte slice
	charsetBytes := []byte(charset)

	// Create a byte slice to hold the random bytes
	randomBytes := make([]byte, 10)

	// Read random bytes from the crypto/rand package
	_, err := rand.Read(randomBytes)
	if err != nil {
		panic(err)
	}

	// Convert random bytes to string using the charset
	for i := 0; i < 10; i++ {
		randomBytes[i] = charsetBytes[int(randomBytes[i])%len(charsetBytes)]
	}

	return fmt.Sprintf("%d", time.Now().UnixMilli()) + string(randomBytes)
}
