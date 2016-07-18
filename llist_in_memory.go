package persisted

type node struct {
	previous *node
	next     *node
	data     *interface{}
}

// The in-memory linked list which backs the persisted version.
type inMemLinkedList struct {
	head   *node
	tail   *node
	length int
}

func (ll *inMemLinkedList) append(newElement interface{}) {
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

func (ll *inMemLinkedList) push(newElement interface{}) {
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

func (ll *inMemLinkedList) pop() interface{} {
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

func (ll *inMemLinkedList) get(position int) interface{} {
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

func (ll *inMemLinkedList) iterator() func() interface{} {
	currNode := ll.head

	return func() interface{} {
		if currNode == nil {
			return nil
		}
		dataToReturn := currNode.data
		currNode = currNode.next
		return *dataToReturn
	}
}
