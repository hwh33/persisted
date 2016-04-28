package persisted

import (
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
		err = ll.Append(newInteger(i))
		if err != nil {
			t.Fatal(err)
		}
	}

	// Now create a new LinkedList from the existing one's file and compare.
	llFromFile, err := NewLinkedList(ll.log.Name(), decodeInt)
	if err != nil {
		t.Fatal(err)
	}
	llInts := getIntegerSlice(ll)
	llFromFileInts := getIntegerSlice(llFromFile)
	if len(llInts) != len(llFromFileInts) {
		t.Fatal("LinkedList loaded from file does not have expected number of elements")
	}
	for i := range llInts {
		if llInts[i] != llFromFileInts[i] {
			t.Error("LinkedList loaded from file does not match expected structure")
		}
	}
}

// Try constructing a LinkedList using a non-existing file in a non-existing
// directory. This should fail.
func TestNonCreatableFile(t *testing.T) {
	t.Parallel()

	_, err := NewLinkedList("non-existing-directory/temp", decodeInt)
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
	tempFile.WriteString(newInteger(1).ToString())

	// Set no permissions whatsoever for this file.
	os.Chmod(tempFile.Name(), 000)

	_, err = NewLinkedList(tempFile.Name(), decodeInt)
	if err == nil {
		t.Error("Constructor should have reported error for non-readable file")
	}
}

// Give the LinkedList constructor a file with lines which cannot be parsed by
// the input DecodeFunction. This should fail.
func TestBadInputFile(t *testing.T) {
	t.Parallel()

	tempFile, err := ioutil.TempFile(".", "temp-testing")
	if err != nil {
		t.Fatal(err)
	}
	defer tempFile.Close()
	defer os.Remove(tempFile.Name())

	// We need to write some data to the file so that the constructor tries to
	// read it.
	tempFile.WriteString("bogus string")

	_, err = NewLinkedList(tempFile.Name(), decodeInt)
	if err == nil {
		t.Error("Constructor should have reported error for badly-formatted file")
	}
}

// Helper function. Assumes that all elements of llist are of type integer (see
// llist_test.go). Returns the integer form of all elements in-order as a slice.
func getIntegerSlice(llist *LinkedList) []int {
	ints := make([]int, llist.Length())
	for currentIndex := 0; currentIndex < llist.Length(); currentIndex++ {
		ints[currentIndex] = llist.Get(currentIndex).(*integer).wrappedInt
	}
	return ints
}
