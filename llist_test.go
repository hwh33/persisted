package persisted

// These tests verify that LinkedList has the standard functionality expected of
// a linked list.

import (
	"strconv"
	"testing"
)

// This struct is just an integer which implements the Stringable interface.
type integer struct {
	wrappedInt int
}

func newInteger(i int) *integer {
	newInteger := new(integer)
	newInteger.wrappedInt = i
	return newInteger
}

func (i *integer) ToString() string {
	return strconv.Itoa(i.wrappedInt)
}

func (i *integer) FromString(s string) error {
	encodedInt, err := strconv.Atoi(s)
	i.wrappedInt = encodedInt
	return err
}

func TestAppendAndGet(t *testing.T) {
	t.Parallel()
	ll := new(LinkedList)
	// Insert 10 elements with data equal to their position, then check the list's
	// length and the value of each element.
	for i := 0; i < 10; i++ {
		ll.Append(newInteger(i))
	}
	if ll.Length() != 10 {
		t.Error("Inserted 10 elements, length was not 10")
	}
	for i := 0; i < 10; i++ {
		element := ll.Get(i).(*integer)
		if element.wrappedInt != i {
			t.Error("Expected: " + strconv.Itoa(i) + ", got: " + strconv.Itoa(element.wrappedInt))
		}
	}
	if ll.Length() != 10 {
		t.Error("Length should not have changed after Get calls")
	}
	// Confirm that calling Get on an invalid index returns nil.
	if ll.Get(100) != nil || ll.Get(-1) != nil {
		t.Error("Get should return nil for invalid index")
	}
}

func TestPushAndPop(t *testing.T) {
	t.Parallel()
	ll := new(LinkedList)
	// Push 10 elements. The data stored in each element reflects the order in
	// which it was added.
	for i := 0; i < 10; i++ {
		ll.Push(newInteger(i))
	}
	if ll.Length() != 10 {
		t.Error("Inserted 10 elements, length was not 10")
	}
	for i := 0; i < 10; i++ {
		element := ll.Pop().(*integer)
		if element.wrappedInt != i {
			t.Error("Expected: " + strconv.Itoa(i) + ", got: " + strconv.Itoa(element.wrappedInt))
		}
	}
	if ll.Length() != 0 {
		t.Error("List should be empty after Pop calls")
	}
	// Confirm that calling Pop on an empty list returns nil.
	if ll.Pop() != nil {
		t.Error("Calling Pop on an empty list should return nil")
	}
}

func TestIterator(t *testing.T) {
	t.Parallel()
	ll := new(LinkedList)
	// Create a list with 10 elements, then create an iterator and test that the
	// iterator returns the elements expected.
	for i := 0; i < 10; i++ {
		ll.Append(newInteger(i))
	}
	// Sanity check.
	if ll.Length() != 10 {
		t.Fatal("Expected list length to be 10")
	}
	iter := ll.Iterator()
	for i := 0; i < 10; i++ {
		element := iter().(*integer)
		if element.wrappedInt != i {
			t.Error("Expected: " + strconv.Itoa(i) + ", got: " + strconv.Itoa(element.wrappedInt))
		}
	}
	// Confirm that the iterator returns nil when it has exhausted the list.
	if iter() != nil {
		t.Error("Iterator should have returned nil after exhausting list")
	}
	// Confirm that the list is untouched.
	for i := 0; i < 10; i++ {
		element := ll.Get(i).(*integer)
		if element.wrappedInt != i {
			t.Error("Expected: " + strconv.Itoa(i) + ", got: " + strconv.Itoa(element.wrappedInt))
		}
	}
	if ll.Length() != 10 {
		t.Error("Length should not have changed after Get calls")
	}
}
