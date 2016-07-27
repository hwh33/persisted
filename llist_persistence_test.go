package persisted

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

// These tests verify that LinkedList is persisted to disk as expected.

// Make a LinkedList anchored to a file. Then try to load a new LinkedList from
// this file and compare the data in each. They should be the same.
func TestPersistence(t *testing.T) {
	t.Parallel()

	ll, wipeTempFiles, err := createTemporaryLinkedList()
	if err != nil {
		t.Fatal(err)
	}
	defer wipeTempFiles()

	// Append 10 elements to the list. Their values reflect their position.
	for i := 0; i < 10; i++ {
		// err = ll.Append(integer{i})
		err = ll.Append(i)
		if err != nil {
			t.Fatal(err)
		}
	}
	// We'll do a few pushes and pops as well.
	for i := 10; i < 20; i += 2 {
		// 2 pushes + 1 pop each loop.
		// err = ll.Push(integer{i})
		err = ll.Push(i)
		if err != nil {
			t.Fatal(err)
		}
		// err = ll.Push(integer{i + 1})
		err = ll.Push(i + 1)
		if err != nil {
			t.Fatal(err)
		}
		_, err = ll.Pop()
		if err != nil {
			t.Fatal(err)
		}
	}

	// Now create a new LinkedList from the existing one's file and compare.
	llJr, err := NewLinkedList(ll.log.file.Name())
	if err != nil {
		t.Fatal(err)
	}
	llInts := getIntegerSlice(ll)
	llJrInts := getIntegerSlice(llJr)
	if len(llInts) != len(llJrInts) {
		t.Fatal("LinkedList loaded from file does not have expected number of elements")
	}
	for i := range llInts {
		if llInts[i] != llJrInts[i] {
			t.Error("LinkedList loaded from file does not match expected structure")
		}
	}

	// Create another LinkedList off the new one and compare again to make sure
	// there were no errors in re-writing the log.
	llTheThird, err := NewLinkedList(llJr.log.file.Name())
	if err != nil {
		t.Fatal(err)
	}
	llTheThirdInts := getIntegerSlice(llTheThird)
	if len(llTheThirdInts) != len(llJrInts) {
		t.Fatal("LinkedList loaded from file does not have expected number of elements")
	}
	for i := range llTheThirdInts {
		if llTheThirdInts[i] != llJrInts[i] {
			t.Error("LinkedList loaded from file does not match expected structure")
		}
	}
}

// Try constructing a LinkedList using a non-existing file in a non-existing
// directory. This should fail.
func TestNonCreatableFile(t *testing.T) {
	t.Parallel()

	_, err := NewLinkedList("non-existing-directory/temp")
	if err == nil {
		t.Error("Constructor should have reported error for non-instantiable file")
	}
}

// Give the LinkedList constructor a file which it does not have permission to
// read. This should fail.
func TestNonReadableFile(t *testing.T) {
	t.Parallel()

	tempFile, err := ioutil.TempFile(".", "temp-testing")
	if err != nil {
		t.Fatal(err)
	}
	defer tempFile.Close()
	defer os.Remove(tempFile.Name())

	// We need to write some data to the file so that the constructor tries to
	// read it.
	// bytes, err := json.Marshal(integer{1})
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// _, err = tempFile.Write(bytes)
	// if err != nil {
	// 	t.Fatal(err)
	// }

	// Set no permissions whatsoever for this file.
	os.Chmod(tempFile.Name(), 000)

	_, err = NewLinkedList(tempFile.Name())
	if err == nil {
		t.Error("Constructor should have reported error for non-readable file")
	}
}

func TestNonWritableFile(t *testing.T) {
	t.Parallel()

	// Create a LinkedList with some data in it.
	ll, wipeTempFiles, err := createTemporaryLinkedList()
	if err != nil {
		t.Fatal(err)
	}
	defer wipeTempFiles()
	ll.Append(integer{1})

	// Now make the log file read-only and try to re-create a LinkedList from it.
	os.Chmod(ll.log.file.Name(), 0444)
	_, err = NewLinkedList(ll.log.file.Name())
	if err == nil {
		t.Error("Constructor should have reported error for non-writable file")
	}
}

// Give the LinkedList constructor a file which does not match the expected
// format.
func TestBadInputFile(t *testing.T) {
	t.Parallel()

	tempFile, err := ioutil.TempFile(".", "temp-testing")
	if err != nil {
		t.Fatal(err)
	}
	defer tempFile.Close()
	defer os.Remove(tempFile.Name())

	_, err = tempFile.WriteString("\nbogus string")
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewLinkedList(tempFile.Name())
	if err == nil {
		t.Error("Constructor should have reported error for badly-formatted file")
	}
}

// Helper function. Assumes that all elements of llist are of type integer (see
// llist_standard_test.go). Returns the integer form of all elements in-order as
// a slice.
func getIntegerSlice(llist *LinkedList) []int {
	ints := make([]int, llist.Length())
	for currentIndex := 0; currentIndex < llist.Length(); currentIndex++ {
		// ints[currentIndex] = llist.Get(currentIndex).(integer).WrappedInt
		fmt.Println("llist.Get:")
		fmt.Println(llist.Get(currentIndex))
		ints[currentIndex] = llist.Get(currentIndex).(int)
	}
	return ints
}
