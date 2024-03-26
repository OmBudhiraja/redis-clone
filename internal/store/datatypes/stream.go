package datatypes

type Entry struct {
	Values map[string]string
	Id     string
}

type Stream struct {
	DataType string
	Values   []*Entry
}

func (s *Stream) GetType() string {
	return s.DataType
}

func (s *Stream) AddEntry(entryId string, pairs []string) {

	entry := &Entry{
		Id:     entryId,
		Values: make(map[string]string),
	}

	for i := 0; i < len(pairs); i += 2 {
		entry.Values[pairs[i]] = pairs[i+1]
	}

	s.Values = append(s.Values, entry)
}
