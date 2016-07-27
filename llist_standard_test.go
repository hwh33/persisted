package persisted

// These tests verify that LinkedList has the standard functionality expected of
// a linked list.

import (
	"io/ioutil"
	"os"
	"strconv"
	"testing"
)

// This struct is just an integer which implements the Stringable interface.
type integer struct {
	WrappedInt int
}

func TestAppendAndGet(t *testing.T) {
	t.Parallel()

	ll, wipeTempFiles, err := createTemporaryLinkedList()
	if err != nil {
		t.Fatal(err)
	}
	defer wipeTempFiles()

	// Insert 10 elements with data equal to their position, then check the list's
	// length and the value of each element.
	for i := 0; i < 10; i++ {
		err := ll.Append(integer{i})
		if err != nil {
			t.Fatal(err)
		}
	}
	if ll.Length() != 10 {
		t.Error("Inserted 10 elements, length was not 10")
	}
	for i := 0; i < 10; i++ {
		element := ll.Get(i).(integer)
		if element.WrappedInt != i {
			t.Error("Expected: " + strconv.Itoa(i) + ", got: " + strconv.Itoa(element.WrappedInt))
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

	ll, wipeTempFiles, err := createTemporaryLinkedList()
	if err != nil {
		t.Fatal(err)
	}
	defer wipeTempFiles()

	// Push 10 elements. The data stored in each element reflects the order in
	// which it was pushed. This should result in a list from 9 to 0.
	for i := 0; i < 10; i++ {
		err = ll.Push(integer{i})
		if err != nil {
			t.Fatal(err)
		}
	}
	if ll.Length() != 10 {
		t.Error("Inserted 10 elements, length was not 10")
	}

	var element interface{}
	numberElements := ll.Length()
	for i := 0; i < numberElements; i++ {
		element, err = ll.Pop()
		if err != nil {
			t.Fatal(err)
		}
		elementAsInteger := element.(integer)
		if elementAsInteger.WrappedInt != i {
			t.Error("Expected: " + strconv.Itoa(i) + ", got: " +
				strconv.Itoa(elementAsInteger.WrappedInt))
		}
	}
	if ll.Length() != 0 {
		t.Error("List should be empty after Pop calls")
	}
	// Confirm that calling Pop on an empty list returns nil.
	popped, err := ll.Pop()
	if err != nil {
		t.Fatal(err)
	}
	if popped != nil {
		t.Error("Calling Pop on an empty list should return nil")
	}
}

func TestIterator(t *testing.T) {
	t.Parallel()

	ll, wipeTempFiles, err := createTemporaryLinkedList()
	if err != nil {
		t.Fatal(err)
	}
	defer wipeTempFiles()

	// Create a list with 10 elements, then create an iterator and test that the
	// iterator returns the elements expected.
	for i := 0; i < 10; i++ {
		err := ll.Append(integer{i})
		if err != nil {
			t.Fatal(err)
		}
	}
	// Sanity check.
	if ll.Length() != 10 {
		t.Fatal("Expected list length to be 10")
	}
	iter := ll.Iterator()
	for i := 0; i < 10; i++ {
		element := iter().(integer)
		if element.WrappedInt != i {
			t.Error("Expected: " + strconv.Itoa(i) + ", got: " + strconv.Itoa(element.WrappedInt))
		}
	}
	// Confirm that the iterator returns nil when it has exhausted the list.
	if iter() != nil {
		t.Error("Iterator should have returned nil after exhausting list")
	}
	// Confirm that the list is untouched.
	for i := 0; i < 10; i++ {
		element := ll.Get(i).(integer)
		if element.WrappedInt != i {
			t.Error("Expected: " + strconv.Itoa(i) + ", got: " + strconv.Itoa(element.WrappedInt))
		}
	}
	if ll.Length() != 10 {
		t.Error("Length should not have changed after Get calls")
	}
}

func createTemporaryLinkedList() (linkedList *LinkedList, wipeTempFiles func() error, err error) {
	// Create a temporary file to anchor the LinkedList to.
	tempFile, err := ioutil.TempFile("", "temp-testing")
	if err != nil {
		return
	}

	wipeTempFiles = func() error {
		err := tempFile.Close()
		if err != nil {
			return err
		}
		err = os.Remove(tempFile.Name())
		if err != nil {
			return err
		}
		return nil
	}

	linkedList, err = NewLinkedList(tempFile.Name())
	return
}
