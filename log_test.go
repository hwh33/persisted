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
			ops[index] = createOp(appendKey, i)
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
		err = l.add(createOp(appendKey, i))
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
			ops[index] = createOp(appendKey, i)
		}
		return ops
	}
	newLog, err := newLog(tf.Name(), newCallback, json.Marshal, json.Unmarshal)
	if err != nil {
		t.Fatal(err)
	}

	operationsMap := make(map[string]func(...interface{}) error)
	operationsMap[appendKey] = func(params ...interface{}) error {
		if len(params) != 1 {
			return fmt.Errorf("Received %d parameters; expected 1", len(params))
		}
		slice, ok := params[0].([]interface{})
		if !ok {
			return fmt.Errorf("Received paramater of type %T; expected []interface{}", params[0])
		}
		i, ok := slice[0].(float64)
		if !ok {
			return fmt.Errorf("Received paramater of type %T; expected float64", slice[0])
		}
		newS = append(newS, int(i))
		return nil
	}
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
	// TODO: implement me!
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
