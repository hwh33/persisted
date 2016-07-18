package persisted

import (
	"encoding/json"
	"fmt"
)

// Operations we record in the log file.
const (
	_append = "__append__"
	_push   = "__push__"
	_pop    = "__pop__"
)

// TODO: either handle newlines / carriage returns or disallow them

// LinkedList is a persisted, doubly-linked list of nodes. Each node can hold
// data, so long as that data implements the Stringable interface. Initialize a
// LinkedList by calling NewLinkedList.
type LinkedList struct {
	inner *inMemLinkedList
	log   *log
}

// NewLinkedList returns a new LinkedList anchored to the file specified by
// the input filepath.
//
// If this file exists and is not empty, it is assumed that the file represents
// a persisted LinkedList and the data structure will be re-constructed. If this
// file does not exist or is empty, a new, empty LinkedList will be created. In
// this case, a new file may be created by this constructor, but all parent
// directories must already exist.
//
// The input DecodeFunction tells the LinkedList how to read the Stringable
// types back from their marshalled form. It should be able to handle any
// Stringables already encoded in the input file.
func NewLinkedList(filepath string) (linkedList *LinkedList, err error) {
	// Initialize the log with the input file path.
	linkedList.log, err = newLog(filepath, linkedList.getCallback(), json.Marshal, json.Unmarshal)
	if err != nil {
		return nil, err
	}
	// Initialize the inner linked list and populate it using the log.
	linkedList.inner = new(inMemLinkedList)
	err = linkedList.log.replay(linkedList.getOperationsMap())
	if err != nil {
		return nil, err
	}
	return linkedList, nil
}

// Append adds the input element to the end of the list.
func (ll *LinkedList) Append(newElement interface{}) error {
	ll.inner.append(newElement)
	return ll.log.add(newOperation(_append, newElement))
}

// Push adds the input element to the beginning of the list.
func (ll *LinkedList) Push(newElement interface{}) error {
	ll.inner.push(newElement)
	return ll.log.add(newOperation(_push, newElement))
}

// Pop removes and returns the last element of the list. Returns nil if the list
// is empty.
func (ll *LinkedList) Pop() (interface{}, error) {
	popped := ll.inner.pop()
	if popped == nil {
		return nil, nil
	}
	return popped, ll.log.add(newOperation(_pop))
}

// Get returns the element at the input position without removing it from the
// list. Returns nil if there is no element at the given position.
func (ll *LinkedList) Get(position int) interface{} {
	return ll.inner.get(position)
}

// Length returns the number of elements in the list.
func (ll *LinkedList) Length() int {
	return ll.inner.length
}

// Iterator returns a function which, when called, returns the next element in
// the list. The iterator function begins at the first element and returns nil
// when it has run out of elements. Uses the underlying structure, so behavior
// is undefined if the list is modified between calls to the iterator function.
func (ll *LinkedList) Iterator() func() interface{} {
	return ll.inner.iterator()
}

// Returns a callback function for the linked list which can be passed into the
// newLog function.
func (ll *LinkedList) getCallback() func() []operation {
	return func() []operation {
		ops := make([]operation, ll.Length())
		iter := ll.Iterator()
		for i := 0; i < ll.Length(); i++ {
			// TODO: make sure there's a solid unit test for Iterator()
			ops[i] = newOperation(_append, iter())
		}
		return ops
	}
}

func (ll *LinkedList) getOperationsMap() map[string]func(...interface{}) error {
	opsMap := make(map[string]func(...interface{}) error)
	opsMap[_append] = func(inputs ...interface{}) error {
		if len(inputs) != 1 {
			return fmt.Errorf("Expected 1 parameter. Received %d.", len(inputs))
		}
		ll.inner.append(inputs[0])
		return nil
	}
	opsMap[_pop] = func(inputs ...interface{}) error {
		if len(inputs) != 0 {
			return fmt.Errorf("Expected 0 parameter. Received %d.", len(inputs))
		}
		ll.inner.pop()
		return nil
	}
	opsMap[_append] = func(inputs ...interface{}) error {
		if len(inputs) != 1 {
			return fmt.Errorf("Expected 1 parameter. Received %d.", len(inputs))
		}
		ll.inner.push(inputs[0])
		return nil
	}
	return opsMap
}
