package persisted

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

func TestNewLogAndReplay(t *testing.T) {
	appendKey := "append"

	tf, err := ioutil.TempFile("", "temp-testing")
	defer os.Remove(tf.Name())
	if err != nil {
		t.Fatal(err)
	}

	// Try making a log for a slice of ints.
	var s []int
	callback := func() []operation {
		ops := make([]operation, len(s))
		for index, i := range s {
			ops[index] = newOperation(appendKey, i)
		}
		return ops
	}
	l, err := newLog(tf.Name(), callback, json.Marshal, json.Unmarshal)
	if err != nil {
		t.Fatal(err)
	}

	// Perform some operations on the slice and record them in the log.
	for i := 0; i < 10; i++ {
		s = append(s, i)
		err = l.add(newOperation(appendKey, i))
		if err != nil {
			t.Fatal(err)
		}
	}

	// Now try to create a new log off of the same file and replay it into a new
	// slice. The result should be a copy of our original slice.
	var newS []int
	newCallback := func() []operation {
		ops := make([]operation, len(newS))
		for index, i := range newS {
			ops[index] = newOperation(appendKey, i)
		}
		return ops
	}
	newLog, err := newLog(tf.Name(), newCallback, json.Marshal, json.Unmarshal)
	if err != nil {
		t.Fatal(err)
	}

	operationsMap := make(map[string]func(...interface{}) error)
	operationsMap[appendKey] = bind(appendOperation, &newS)
	err = newLog.replay(operationsMap)
	if err != nil {
		t.Fatal(err)
	}

	if len(s) != len(newS) {
		t.Fatalf("Length of slices should be equivalent: len(s): %d; len(newS): %d",
			len(s), len(newS))
	}

	for i := 0; i < len(s); i++ {
		if s[i] != newS[i] {
			t.Fatal("Slices should be equivalent")
		}
	}
}

func TestAdd(t *testing.T) {
	appendKey := "append"
	deleteKey := "delete"
	replaceKey := "replace"

	var s []int
	operationsMap := make(map[string]func(...interface{}) error)
	operationsMap[appendKey] = bind(appendOperation, &s)
	operationsMap[deleteKey] = bind(deleteOperation, &s)
	operationsMap[replaceKey] = bind(replaceOperation, &s)

	tf, err := ioutil.TempFile("", "temp-testing")
	defer os.Remove(tf.Name())
	if err != nil {
		t.Fatal(err)
	}

	// Make a log for s.
	callback := func() []operation {
		ops := make([]operation, len(s))
		for index, i := range s {
			ops[index] = newOperation(appendKey, i)
		}
		return ops
	}
	l, err := newLog(tf.Name(), callback, json.Marshal, json.Unmarshal)
	if err != nil {
		t.Fatal(err)
	}

	// Perform a series of operations and log each one.
	for i := 0; i < 10; i++ {
		s = append(s, i)
		l.add(newOperation(appendKey, i))
	}
	for i := 1; i < 10; i += 2 {
		s[i] = 100
		l.add(newOperation(replaceKey, i, 100))
	}
	s = append(s[:5], s[6:]...)
	l.add(newOperation(deleteKey, 5))

	// Now we test the accuracy of the log. We copy s over to sCopy and clear
	// out s. Then we replay the log, which will rebuild s. Finally, we compare
	// s and sCopy.
	sCopy := make([]int, len(s))
	copy(sCopy, s)
	// Sanity check.
	if !slicesEqual(s, sCopy) {
		t.Fatal("Slices should be identical at this point")
	}
	s = make([]int, 0)
	if len(s) != 0 {
		t.Fatal("Slice s should have been wiped out")
	}
	err = l.replay(operationsMap)
	if err != nil {
		t.Fatal(err)
	}
	if !slicesEqual(s, sCopy) {
		t.Fatal("Log did not accurately reflect state")
	}
}

func TestCompact(t *testing.T) {
	// TODO: implement me!
}

func TestCompactIfNecessary(t *testing.T) {
	// TODO: implement me!
}

func TestOperationRoundtrip(t *testing.T) {
	// TODO: implement me!
}

// -- Helper functions --

func appendOperation(params ...interface{}) error {
	slicePtr, ints, err := doTypeAssertions(2, params)
	if err != nil {
		return err
	}
	*slicePtr = append(*slicePtr, ints[0])
	return nil
}

func deleteOperation(params ...interface{}) error {
	slicePtr, ints, err := doTypeAssertions(2, params)
	if err != nil {
		return err
	}
	indexToDelete := ints[0]
	*slicePtr = append((*slicePtr)[:indexToDelete], (*slicePtr)[indexToDelete+1:]...)
	return nil
}

func replaceOperation(params ...interface{}) error {
	slicePtr, ints, err := doTypeAssertions(3, params)
	if err != nil {
		return err
	}
	indexToReplace := ints[0]
	replacement := ints[1]
	(*slicePtr)[indexToReplace] = replacement
	return nil
}

func doTypeAssertions(expectedLength int, params []interface{}) (*[]int, []int, error) {
	if len(params) != expectedLength {
		return nil, nil, fmt.Errorf("Received %d parameters; expected %d", len(params), expectedLength)
	}
	slice, ok := params[0].(*[]int)
	if !ok {
		return nil, nil, fmt.Errorf("Received parameter of type %T; expected *[]int", params[0])
	}
	var ints []int
	for _, param := range params[1:] {
		i, ok := param.(float64)
		if !ok {
			return nil, nil, fmt.Errorf("Received paramater of type %T; expected float64", params[1])
		}
		ints = append(ints, int(i))
	}
	return slice, ints, nil
}

// Binds the input parameters to the closure.
func bind(closure func(...interface{}) error, params ...interface{}) func(...interface{}) error {
	return func(unboundParams ...interface{}) error {
		return closure(append(params, unboundParams...)...)
	}
}

func slicesEqual(slice1, slice2 []int) bool {
	if len(slice1) != len(slice2) {
		return false
	}
	for i := range slice1 {
		if slice1[i] != slice2[i] {
			return false
		}
	}
	return true
}
