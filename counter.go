package crudite

import (
	"github.com/pkg/errors"
)

const (
	pCounterIDSuffix = "-pcounter"
	nCounterIDSuffix = "-ncounter"
)

// Counter implements a PN Counter CRDT
type Counter struct {
	Name string
	tm   *TypeManager
	d    *abstractCRDT
}

// ErrNoSuchCounter is returned when a named counter doesn't exist
var ErrNoSuchCounter = errors.New("no such counter found")

// GetCounter returns a counter that already exists in the log
func (tm *TypeManager) GetCounter(name string) (*Counter, error) {
	tm.contains.RLock()
	defer tm.contains.RUnlock()
	if abstract, ok := tm.contains.abstracts[name]; ok && abstract.dtype == PNCounter {
		return &Counter{
			Name: name,
			tm:   tm,
			d:    abstract,
		}, nil
	}
	return nil, ErrNoSuchCounter
}

// NewCounter creates and returns a new Counter if it doesn't exist, or the existing counter with that name.
func (tm *TypeManager) NewCounter(name string) *Counter {
	c := &Counter{
		Name: name,
		tm:   tm,
	}

	tm.contains.RLock()
	if abstract, ok := tm.contains.abstracts[name]; ok && abstract.dtype == PNCounter {
		c.d = abstract
		tm.contains.RUnlock()
		return c
	}
	tm.contains.RUnlock()

	pc := &crdt{
		id:    name + pCounterIDSuffix,
		dtype: pCounter,
	}
	nc := &crdt{
		id:    name + nCounterIDSuffix,
		dtype: nCounter,
	}
	abstract := &abstractCRDT{
		id:    name,
		dtype: PNCounter,
		components: map[DataType]*crdt{
			pCounter: pc,
			nCounter: nc,
		},
	}

	tm.contains.Lock()
	tm.contains.values[pc.id] = pc
	tm.contains.values[nc.id] = nc
	tm.contains.abstracts[name] = abstract
	tm.contains.Unlock()

	c.d = abstract
	return c
}

// Increment increments the counter by n. Pass a negative n to decrement.
func (c *Counter) Increment(n int) error {
	var v *crdt
	var val uint64
	switch {
	case n > 0:
		v = c.d.components[pCounter]
		val = uint64(n)
	case n < 0:
		v = c.d.components[nCounter]
		val = uint64(-n)
	case n == 0:
		return nil
	}
	err := c.incMsg(v, uint(val)) // send increment msg first, if it fails we do not want to locally increment
	if err != nil {
		return err
	}
	v.Lock()
	v.cvalue += val
	v.Unlock()
	return nil
}

// Value returns the current value of the counter
func (c *Counter) Value() int {
	c.d.components[pCounter].RLock()
	c.d.components[nCounter].RLock()
	defer func() { c.d.components[pCounter].RUnlock(); c.d.components[nCounter].RUnlock() }()
	return int(c.d.components[pCounter].cvalue - c.d.components[nCounter].cvalue)
}

// incMsg sends the increment message to the log.
func (c *Counter) incMsg(cd *crdt, n uint) error {
	cd.RLock()
	opm := &operationMessage{
		ID:           cd.id,
		AbstractID:   c.Name,
		AbstractType: c.d.dtype,
		ManagerID:    c.tm.id,
		Type:         cd.dtype,
		Delta:        n,
		Message:      opMsg,
	}
	cd.RUnlock()
	return c.tm.sendop(opm)
}
