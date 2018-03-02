package crudite

import (
	"encoding/hex"
	"time"

	"github.com/pkg/errors"
)

const (
	aSetIDSuffix = "-aset"
	rSetIDSuffix = "-rset"
)

// Set implements a LWW set (removal-biased)
type Set struct {
	Name string
	tm   *TypeManager
	d    *abstractCRDT
}

// ErrNoSuchSet is returned when a named set doesn't exist
var ErrNoSuchSet = errors.New("no such set found")

// GetSet returns a set that already exists in the log
func (tm *TypeManager) GetSet(name string) (*Set, error) {
	tm.contains.RLock()
	defer tm.contains.RUnlock()
	if abstract, ok := tm.contains.abstracts[name]; ok && abstract.dtype == LWWSet {
		return &Set{
			Name: name,
			tm:   tm,
			d:    abstract,
		}, nil
	}
	return nil, ErrNoSuchSet
}

// NewSet creates and returns a new Set if it doesn't exist, or the existing set with that name.
func (tm *TypeManager) NewSet(name string) *Set {
	s := &Set{
		Name: name,
		tm:   tm,
	}

	tm.contains.RLock()
	if abstract, ok := tm.contains.abstracts[name]; ok && abstract.dtype == LWWSet {
		s.d = abstract
		tm.contains.RUnlock()
		return s
	}
	tm.contains.RUnlock()

	as := &crdt{
		id:    name + aSetIDSuffix,
		dtype: aSet,
		sval:  setValue{},
	}
	rs := &crdt{
		id:    name + rSetIDSuffix,
		dtype: rSet,
		sval:  setValue{},
	}
	abstract := &abstractCRDT{
		id:    name,
		dtype: LWWSet,
		components: map[DataType]*crdt{
			aSet: as,
			rSet: rs,
		},
	}

	tm.contains.Lock()
	tm.contains.values[as.id] = as
	tm.contains.values[rs.id] = rs
	tm.contains.abstracts[name] = abstract
	tm.contains.Unlock()

	s.d = abstract
	return s
}

func (s *Set) add(elem []byte, dt DataType) error {
	s.d.components[dt].RLock()
	op := &operationMessage{
		ID:           s.d.components[dt].id,
		AbstractID:   s.d.id,
		AbstractType: s.d.dtype,
		ManagerID:    s.tm.id,
		Type:         s.d.components[dt].dtype,
		Message:      opMsg,
		Element:      elem,
	}
	s.d.components[dt].RUnlock()
	if err := s.tm.sendop(op); err != nil {
		return errors.Wrap(err, "error sending op msg")
	}
	s.d.components[dt].Lock()
	s.d.components[dt].sval.add(elem, time.Now().UTC())
	s.d.components[dt].Unlock()
	return nil
}

// current constructs the current state of the set
func (s *Set) current() setValue {
	s.d.components[aSet].RLock()
	s.d.components[rSet].RLock()
	defer func() { s.d.components[aSet].RUnlock(); s.d.components[rSet].RUnlock() }()
	output := setValue{}
	for elem, ts := range s.d.components[aSet].sval {
		if rts, ok := s.d.components[rSet].sval[elem]; !ok || rts.Before(ts) {
			output[elem] = ts
		}
	}
	return output
}

// Add adds elem to the set
func (s *Set) Add(elem []byte) error {
	return s.add(elem, aSet)
}

// Remove removes elem from the set
func (s *Set) Remove(elem []byte) error {
	return s.add(elem, rSet)
}

// Contains indicates whether elem exists in the set
func (s *Set) Contains(elem []byte) bool {
	cur := s.current()
	_, ok := cur.contains(elem)
	return ok
}

// Elements returns all the elements in the set in no particular order
func (s *Set) Elements() [][]byte {
	cur := s.current()
	output := make([][]byte, len(cur))
	i := 0
	for es := range cur {
		eb, _ := hex.DecodeString(es)
		output[i] = eb
		i++
	}
	return output
}
