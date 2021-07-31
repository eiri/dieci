package dieci

// Datalog represents a datastore's datalog
type Datalog struct {
	backend Backend
}

// NewDatalog returns a new datalog for a given transaction
func NewDatalog(b Backend) *Datalog {
	return &Datalog{backend: b}
}

// Read is a read callback
func (dl *Datalog) Read(score Score) ([]byte, error) {
	return dl.backend.Read(score)
}

// Write is a write callback
func (dl *Datalog) Write(data []byte) (Score, error) {
	score := NewScore(data)
	if ok, err := dl.backend.Exists(score); ok {
		return score, nil
	} else if err != nil {
		return Score{}, err
	}

	err := dl.backend.Write(score, data)
	if err != nil {
		return Score{}, err
	}
	return score, nil
}
