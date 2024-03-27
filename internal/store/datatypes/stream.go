package datatypes

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

type Entry struct {
	Values  map[string]string
	Id      string
	majorId int
	minorId int
}

type Stream struct {
	DataType string
	Values   []Entry
}

func (s *Stream) GetType() string {
	return s.DataType
}

func (s *Stream) AddEntry(entryId string, pairs []string) (string, error) {

	majorId, minorId, err := s.validateEntryId(entryId)

	if err != nil {
		return "", err
	}

	entry := Entry{
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

func (s *Stream) GetRange(startId, endId string) ([]Entry, error) {

	startMajorId, startMinorId, err := s.parseEntryIdForRange(startId, true)

	if err != nil {
		return nil, err
	}

	endMajorId, endMinorId, err := s.parseEntryIdForRange(endId, false)

	if err != nil {
		return nil, err
	}

	if startMajorId == 0 && startMinorId == 0 {
		return nil, errors.New("ERR Invalid start id")
	}

	if endMajorId == 0 && endMinorId == 0 {
		return nil, errors.New("ERR Invalid end id")
	}

	if startMajorId > endMajorId {
		return nil, errors.New("ERR Invalid range")
	}

	if startMajorId == endMajorId && startMinorId > endMinorId {
		return nil, errors.New("ERR Invalid range")
	}

	var entries []Entry

	for _, entry := range s.Values {
		if entry.majorId >= startMajorId && entry.majorId <= endMajorId {

			if entry.minorId >= startMinorId && entry.minorId <= endMinorId {
				entries = append(entries, entry)
			}
		}
	}

	return entries, nil
}

// returns majorId, minorId, error.
// if forStart is true, then it will return minInt for minorId if it is not present.
// if forStart is false, then it will return maxInt for minorId if it is present.
func (s *Stream) parseEntryIdForRange(id string, forStart bool) (int, int, error) {

	var majorId, minorId int
	var err error

	if id == "-" {
		if forStart {
			return math.MinInt, math.MinInt, nil
		} else {
			return 0, 0, errors.New("ERR end id cannot be '-'")
		}
	}

	entryId := strings.Split(id, "-")

	if len(entryId) == 1 {
		if forStart {
			minorId = math.MinInt
		} else {
			minorId = math.MaxInt
		}

	} else {
		minorId, err = strconv.Atoi(entryId[1])

		if err != nil {
			return 0, 0, errors.New("ERR Invalid entry id")
		}
	}

	majorId, err = strconv.Atoi(entryId[0])

	if err != nil {
		return 0, 0, errors.New("ERR Invalid entry id")
	}

	return majorId, minorId, nil
}

func (s *Stream) validateEntryId(id string) (int, int, error) {

	if id == "*" {
		majorId, minorId := s.generateNewEntryId()
		return majorId, minorId, nil
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

	if majorId < lastEntry.majorId {
		return 0, 0, errors.New("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	}

	if majorId == lastEntry.majorId && minorId <= lastEntry.minorId {
		// return 0, 0, errors.New("The ID specified in XADD is equal or smaller than the target stream top item")
		return 0, 0, errors.New("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	}

	return majorId, minorId, nil
}

func (s *Stream) generateNewEntryId() (int, int) {
	majorId := int(time.Now().UnixMilli())

	if len(s.Values) == 0 {
		return majorId, 0
	}

	lastEntry := s.Values[len(s.Values)-1]

	if majorId > lastEntry.majorId {
		return majorId, 0
	}

	return lastEntry.majorId, lastEntry.minorId + 1
}

func (s *Stream) generateMinorEntryId(majorId int) (int, int, error) {
	if len(s.Values) == 0 {
		if majorId == 0 {
			return majorId, 1, nil
		}

		return majorId, 0, nil
	}

	lastEntry := s.Values[len(s.Values)-1]

	if majorId < lastEntry.majorId {
		return 0, 0, errors.New("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	}

	if majorId == lastEntry.majorId {
		return majorId, lastEntry.minorId + 1, nil
	}

	return majorId, 0, nil
}
