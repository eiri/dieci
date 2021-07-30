package dieci

// Datalog represents a datastore's datalog
type Datalog struct {
	backend Backend
}

// NewDatalog returns a new datalog for a given transaction
func NewDatalog(b Backend) *Datalog {
	return &Datalog{backend: b}
}

// read is a read callback
func (dl *Datalog) read(sc score) ([]byte, error) {
	return dl.backend.Read(sc)
}

// write is a write callback
func (dl *Datalog) write(data []byte) (score, error) {
	sc := newScore(data)
	err := dl.backend.Write(sc, data)
	if err != nil {
		return score{}, err
	}
	return sc, nil
}
