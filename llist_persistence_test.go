package persisted

import (
	"io/ioutil"
	"os"
	"strconv"
	"testing"
)

// These tests verify that LinkedList is persisted to disk as expected.

func decodeInt(stringForm string) (Stringable, error) {
	i, err := strconv.Atoi(stringForm)
	if err != nil {
		return nil, err
	}
	return newInteger(i), nil
}

func TestPersistence(t *testing.T) {
	t.Parallel()

	tempFile, err := ioutil.TempFile("", "temp-testing")
	if err != nil {
		t.Fatal(err)
	}
	defer tempFile.Close()
	defer os.Remove(tempFile.Name())

	ll, err := NewLinkedList(tempFile.Name(), decodeInt)
	if err != nil {
		t.Fatal(err)
	}
	// Append 10 elements to the list. Their values reflect their position.
	for i := 0; i < 10; i++ {
		err = ll.Append(newInteger(i))
		if err != nil {
			t.Error(err)
		}
	}

	llFromFile, err := NewLinkedList(tempFile.Name(), decodeInt)
	if err != nil {
		t.Fatal(err)
	}
	llInts := getIntegerSlice(ll)
	llFromFileInts := getIntegerSlice(llFromFile)
	if len(llInts) != len(llFromFileInts) {
		t.Error("LinkedList loaded from file does not have expected number of elements")
	}
	for i := range llInts {
		if llInts[i] != llFromFileInts[i] {
			t.Error("LinkedList loaded from file does not match expected structure")
		}
	}
}

// Helper function. Assumes that all elements of llist are of type integer (see
// llist_test.go). Returns the integer form of all elements in-order as a slice.
func getIntegerSlice(llist *LinkedList) []int {
	ints := make([]int, llist.length)
	for popped := llist.Pop(); popped != nil; popped = llist.Pop() {
		ints = append(ints, popped.(*integer).wrappedInt)
	}
	return ints
}
