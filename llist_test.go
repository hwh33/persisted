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

func (i *integer) ToString() (string, error) {
	s := strconv.Itoa(i.wrappedInt)
	return s, nil
}

func (i *integer) FromString(s string) error {
	encodedInt, err := strconv.Atoi(s)
	i.wrappedInt = encodedInt
	return err
}

func TestAppend(t *testing.T) {
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
		element := new(integer)
		stringForm, _ := ll.Get(i).ToString()
		element.FromString(stringForm)
		if element.wrappedInt != i {
			t.Error("Expected: " + strconv.Itoa(i) + ", got: " + strconv.Itoa(element.wrappedInt))
		}
	}
}
