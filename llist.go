package persisted

import (
	"bufio"
	"os"
)

type node struct {
	previous *node
	next     *node
	data     *Stringable
}

// LinkedList is a list of nodes with pointers to each other. Each node can hold
// data, so long as that data implements the Stringable interface. Initialize a
// LinkedList by calling:
//  new(LinkedList)
type LinkedList struct {
	head    *node
	tail    *node
	length  int
	storage *os.File
}

// NewLinkedList returns a new LinkedList anchored to the file specified by
// the input filepath. If this file exists and is not empty, it is assumed
// that this file represents a persisted LinkedList and the data structure
// will be re-constructed. If this file does not exist or is empty, a new,
// empty LinkedList will be created. TODO: parent directories?
func NewLinkedList(filepath string, decodeFn DecodeFunction) (*LinkedList, error) {
	linkedList := new(LinkedList)
	if fileinfo, err := os.Stat(filepath); os.IsNotExist(err) || fileinfo.Size() == 0 {
		// File does not exist or is empty, create a new one.
		linkedList.storage, err = os.Create(filepath)
		if err != nil {
			return nil, err
		}
		return linkedList, nil
	}

	// If we are at this point, we are going to be reconstructing the
	// LinkedList from file.
	err := *new(error)
	linkedList.storage, err = os.Open(filepath)
	if err != nil {
		return nil, err
	}

	scanner := bufio.NewScanner(linkedList.storage)
	for scanner.Scan() {
		// Attempt to decode the current line.
		decodedStringable, err := decodeFn(scanner.Text())
		if err != nil {
			return nil, err
		}
		linkedList.appendWithoutWriting(decodedStringable)
	}
	return linkedList, nil
}

// A special function used while constructing a LinkedList from file. Used
// to avoid writing to file the elements we are currently reading off.
func (ll *LinkedList) appendWithoutWriting(newElement Stringable) {
	newNode := new(node)
	newNode.data = &newElement
	if ll.tail == nil {
		// This is the first element.
		ll.head = newNode
		ll.tail = newNode
		ll.length = 1
	} else {
		ll.tail.next = newNode
		newNode.previous = ll.tail
		ll.tail = newNode
		ll.length++
	}
}

// Append adds the input element to the end of the list.
func (ll *LinkedList) Append(newElement Stringable) error {
	newElementString, err := newElement.ToString()
	if err != nil {
		return err
	}
	ll.appendWithoutWriting(newElement)
	// Jump to the end of the backing file and append the new element.
	ll.storage.Seek(0, 2)
	_, err = ll.storage.WriteString(newElementString + "\n")
	return err
}

// Push adds the input element to the beginning of the list.
func (ll *LinkedList) Push(newElement Stringable) {
	newNode := new(node)
	newNode.data = &newElement
	if ll.head == nil {
		// This is the first element.
		ll.head = newNode
		ll.tail = newNode
		ll.length = 1
	} else {
		ll.head.previous = newNode
		newNode.next = ll.head
		ll.head = newNode
		ll.length++
	}
}

// Pop removes and returns the last element of the list. Returns nil if the list
// is empty.
func (ll *LinkedList) Pop() Stringable {
	if ll.length == 0 {
		return nil
	}
	dataToReturn := ll.tail.data
	ll.tail = ll.tail.previous
	if ll.tail != nil {
		ll.tail.next = nil
	}
	ll.length--

	return *dataToReturn
}

// Get returns the element at the input position without removing it from the
// list. Returns nil if there is no element at the given position.
func (ll *LinkedList) Get(position int) Stringable {
	if position < 0 || ll.length-1 < position {
		// Out of bounds.
		return nil
	}
	currNode := ll.head
	for currPosition := 0; currPosition < position; currPosition++ {
		currNode = currNode.next
	}
	return *currNode.data
}

// Length returns the number of elements in the list.
func (ll *LinkedList) Length() int {
	return ll.length
}

// Iterator returns a function which, when called, returns the next element in
// the list. The iterator function begins at the first element and returns nil
// when it has run out of elements. Uses the underlying structure, so behavior
// is undefined if the list is modified between calls to the iterator function.
func (ll *LinkedList) Iterator() func() Stringable {
	currNode := ll.head

	return func() Stringable {
		if currNode == nil {
			return nil
		}
		dataToReturn := currNode.data
		currNode = currNode.next
		return *dataToReturn
	}
}
