package persisted

type node struct {
	previous *node
	next     *node
	data     *Stringable
}

// LinkedList is a list of nodes with pointers to each other. Each node can hold
// data, so long as that data implements the Stringable interface. Initialize a
// LinkedList by calling
//  new(LinkedList)
type LinkedList struct {
	head   *node
	tail   *node
	length int
}

// Implementation note: the LinkedList struct's zero values are perfectly
// sufficient, so we do not provide a constructor. Rather, we instruct users to
// simply use the built-in new function.

// Append adds the input element to the end of the list.
func (ll *LinkedList) Append(newElement Stringable) {
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
	if ll.length-1 < position {
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
