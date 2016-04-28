package persisted

import (
	"bufio"
	"errors"
	"os"
)

const (
	append = "__append__"
	push   = "__push__"
	pop    = "__pop__"
)

const noSubject = "__no_subject__"

// LinkedList is a list of nodes with pointers to each other. Each node can hold
// data, so long as that data implements the Stringable interface. Initialize a
// LinkedList by calling NewLinkedList.
type LinkedList struct {
	inner *inMemLinkedList
	log   *os.File
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
// Stringables handled by this LinkedList or already encoded in the input file.
func NewLinkedList(filepath string, decodeFn DecodeFunction) (*LinkedList, error) {
	linkedList := new(LinkedList)

	// Rebuild the in-memory list from the input log.
	inputLog, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}

	linkedList.inner = new(inMemLinkedList)
	scanner := bufio.NewScanner(inputLog)
	var decoded Stringable
	for scanner.Scan() {
		action := scanner.Text()
		if err != nil {
			return nil, errors.New("Failed to read input linked list file: " + err.Error())
		}
		decoded, err = decodeFn(scanner.Text())
		if action != pop && err != nil {
			return nil, errors.New("Failed to decode encoded stringable in input file: " + err.Error())
		}

		if action == append {
			linkedList.inner.append(decoded)
		} else if action == push {
			linkedList.inner.push(decoded)
		} else if action == pop {
			linkedList.inner.pop()
		} else {
			return nil, errors.New("Input file appears to be corrupted")
		}
	}

	// Now wipe the log and re-write only the current state of the list.
	// TODO: it would be safer to write this to a temporary file first
	err = inputLog.Close()
	if err != nil {
		return nil, err
	}
	linkedList.log, err = os.Create(filepath)
	if err != nil {
		return nil, err
	}

	iterator := linkedList.Iterator()
	for element := iterator(); element != nil; element = iterator() {
		linkedList.logAction(append, element)
	}

	return linkedList, nil
}

// Append adds the input element to the end of the list.
func (ll *LinkedList) Append(newElement Stringable) error {
	ll.inner.append(newElement)
	return ll.logAction(append, newElement)
}

// Push adds the input element to the beginning of the list.
func (ll *LinkedList) Push(newElement Stringable) error {
	ll.inner.push(newElement)
	return ll.logAction(push, newElement)
}

// Pop removes and returns the last element of the list. Returns nil if the list
// is empty.
func (ll *LinkedList) Pop() (Stringable, error) {
	popped := ll.inner.pop()
	if popped == nil {
		return nil, nil
	}
	return popped, ll.logSimpleAction(pop)
}

// Get returns the element at the input position without removing it from the
// list. Returns nil if there is no element at the given position.
func (ll *LinkedList) Get(position int) Stringable {
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
func (ll *LinkedList) Iterator() func() Stringable {
	return ll.inner.iterator()
}

func (ll *LinkedList) logSimpleAction(action string) error {
	ll.log.Seek(0, os.SEEK_END)
	_, err := ll.log.WriteString(action + "\n" + noSubject + "\n")
	return err
}

func (ll *LinkedList) logAction(action string, subject Stringable) error {
	ll.log.Seek(0, os.SEEK_END)
	_, err := ll.log.WriteString(action + "\n" + subject.ToString() + "\n")
	return err
}
