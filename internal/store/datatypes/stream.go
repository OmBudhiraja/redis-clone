package datatypes

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type Entry struct {
	Values  map[string]string
	Id      string
	majorId int
	minorId int
}

type Stream struct {
	DataType string
	Values   []*Entry
}

func (s *Stream) GetType() string {
	return s.DataType
}

func (s *Stream) AddEntry(entryId string, pairs []string) (string, error) {

	majorId, minorId, err := s.parseEntryId(entryId)

	if err != nil {
		return "", err
	}

	entry := &Entry{
		Id:      fmt.Sprintf("%d-%d", majorId, minorId),
		Values:  make(map[string]string),
		majorId: majorId,
		minorId: minorId,
	}

	for i := 0; i < len(pairs); i += 2 {
		entry.Values[pairs[i]] = pairs[i+1]
	}

	s.Values = append(s.Values, entry)
	return entry.Id, nil
}

func (s *Stream) parseEntryId(id string) (int, int, error) {

	if id == "*" {
		return s.generateNewEntryId()
	}

	entryId := strings.Split(id, "-")

	if len(entryId) != 2 {
		return 0, 0, errors.New("invalid entry id")
	}

	majorId, err := strconv.Atoi(entryId[0])

	if err != nil {
		return 0, 0, errors.New("invalid entry id")
	}

	if entryId[1] == "*" {
		// minorId := 0
		return s.generateMinorEntryId(majorId)
	}

	minorId, err := strconv.Atoi(entryId[1])

	if err != nil {
		return 0, 0, errors.New("invalid entry id")
	}

	if majorId == 0 && minorId == 0 {
		return 0, 0, errors.New("ERR The ID specified in XADD must be greater than 0-0")
	}

	if len(s.Values) == 0 {
		return majorId, minorId, nil
	}

	lastEntry := s.Values[len(s.Values)-1]

	if lastEntry == nil {
		panic("last entry is nil??")
	}

	if majorId < lastEntry.majorId {
		return 0, 0, errors.New("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	}

	if majorId == lastEntry.majorId && minorId <= lastEntry.minorId {
		// return 0, 0, errors.New("The ID specified in XADD is equal or smaller than the target stream top item")
		return 0, 0, fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	}

	return majorId, minorId, nil
}

func (s *Stream) generateNewEntryId() (int, int, error) {
	// TODO: Implement this
	return 0, 0, nil
}

func (s *Stream) generateMinorEntryId(majorId int) (int, int, error) {
	if len(s.Values) == 0 {
		if majorId == 0 {
			return majorId, 1, nil
		}

		return majorId, 0, nil
	}

	lastEntry := s.Values[len(s.Values)-1]

	if lastEntry == nil {
		panic("last entry is nil??")
	}

	if majorId < lastEntry.majorId {
		return 0, 0, fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	}

	if majorId == lastEntry.majorId {
		return majorId, lastEntry.minorId + 1, nil
	}

	return majorId, 0, nil
}
