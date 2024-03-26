package datatypes

import "time"

type String struct {
	DataType string
	Value    string
	Expiry   time.Time
}

func (s *String) GetType() string {
	return s.DataType
}
