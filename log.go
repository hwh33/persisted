package persisted

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
)

// The log type defined in this file is used to actively record the state of
// data structures in the persisted package. A data structure will initialize
// the log at a given filepath, then record each operation which changes its
// state.
// When initializing an existing persisted data structure, the log can be
// replayed to put the structure back in its prior state.
// The log will be compacted upon replay as well as upon reaching certain
// thresholds. This is to keep the log from becoming too long and making replay
// a slow process.

// Initialize the compaction threshold to 10 KB.
const initialCompactionThreshold = 10 * 1024

type log struct {
	file                   *os.File
	getCompactedOperations func() []operation
	compactThreshold       int64
	marshaler              marshalFunc
	unmarshaler            unmarshalFunc
}

// Represents some operation which changes the state of a persisted data
// structure.
type operation struct {
	key        string
	parameters []interface{}
}

// Used to marshal and unmarshal the parameters in an operation.
type marshalFunc func(interface{}) ([]byte, error)
type unmarshalFunc func([]byte, interface{}) error

// Used for JSON encoding / decoding of operations.
type marshalledOperation struct {
	Key                  string
	MarshalledParameters [][]byte
}

// Initializes a log backed by the file at the provided path. If this file
// already exists, it will be interpreted as an existing log. If the file does
// not exist, it will be created, but all parent directories must exist.
//
// compactedOperationsCallback should return the most compact series of
// operations which represent the data structure. This callback function may be
// called multiple times. These calls are synchronous but no guarantees are made
// as to which method calls will result in execution of the callback. The
// returned slice must always represent the current state of the structure.
//
// The marshal and unmarshal functions are used for parameters passed in to the
// add method. These methods must produce valid JSON and a "round-tripped"
// parameter (one which has been marshalled, then unmarshalled) must be
// equivalent to its original self.
func newLog(filepath string, compactedOperationsCallback func() []operation,
	marshalFn marshalFunc, unmarshalFn unmarshalFunc) (*log, error) {
	logFile, err := os.OpenFile(filepath, os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, err
	}
	// TODO: check file
	return &log{
		logFile,
		compactedOperationsCallback,
		initialCompactionThreshold,
		marshalFn,
		unmarshalFn,
	}, nil
}

// Records the state change in the log.
func (l *log) add(op operation) error {
	marshalledOp, err := op.marshal(l.marshaler)
	if err != nil {
		return err
	}
	_, err = l.file.Seek(0, 2)
	if err != nil {
		return err
	}
	err = json.NewEncoder(l.file).Encode(marshalledOp)
	if err != nil {
		return err
	}
	return l.compactIfNecessary()
}

// Replays every operation in the log. The operation key is used to look up the
// associated function in the input map. The function is then called with the
// operation parameters.
// The functions in the map should most likely be closures so that, when
// applied, they have the desired effect on the state of the data structure
// backed by this log.
func (l *log) replay(operationsMap map[string]func(...interface{}) error) error {
	_, err := l.file.Seek(0, 0)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(l.file)
	var marshalledOp marshalledOperation
	for {
		err := decoder.Decode(&marshalledOp)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		op, err := marshalledOp.unmarshal(l.unmarshaler)
		if err != nil {
			return errors.New("Error unmarshalling operation: " + err.Error())
		}
		opFunction, keyExists := operationsMap[op.key]
		if !keyExists {
			return errors.New("Recorded key <" + op.key + "> not found in input map")
		}
		err = opFunction(op.parameters)
		if err != nil {
			return errors.New("Error applying operation: " + err.Error())
		}
	}
	// Compact now as we'd rather take a performance hit during initialization.
	return l.compact()
}

// Compact the log. This is equivalent to calling l.add, in order, for every
// state change returned by l.getCompactedChanges().
func (l *log) compact() error {
	tempFile, err := ioutil.TempFile("", "TemporaryCompactionFile-"+filepath.Base(l.file.Name()))
	if err != nil {
		return err
	}
	ops := l.getCompactedOperations()
	encoder := json.NewEncoder(tempFile)
	for _, op := range ops {
		marshalledOp, err := op.marshal(l.marshaler)
		if err != nil {
			return errors.New("Marshalling error during compaction: " + err.Error())
		}
		err = encoder.Encode(marshalledOp)
		if err != nil {
			return errors.New("Error during compaction: " + err.Error())
		}
	}
	// If all went well, we can now over-write the existing log.
	return os.Rename(tempFile.Name(), l.file.Name())
}

// Compact if size(log) > compaction threshold, otherwise no-op.
func (l *log) compactIfNecessary() error {
	stat, err := l.file.Stat()
	if err != nil {
		return err
	}
	if stat.Size() > l.compactThreshold {
		fmt.Println("compacting")
		err := l.compact()
		if err != nil {
			return err
		}
		stat, err = l.file.Stat()
		if err != nil {
			return err
		}
		// If, after compaction, the log is still over the threshold, then we need to
		// increase the compaction threshold to avoid thrashing. We simply double the
		// threshold each time this happens.
		if stat.Size() > l.compactThreshold {
			l.compactThreshold = l.compactThreshold * 2
		}
	}
	return nil
}

// Convenience function for creating operations.
func createOp(key string, parameters ...interface{}) operation {
	return operation{key, parameters}
}

func (sc *operation) marshal(marshal marshalFunc) (marshalledOp marshalledOperation, err error) {
	marshalledParameters := make([][]byte, len(sc.parameters))
	for index, parameter := range sc.parameters {
		marshalledParameters[index], err = marshal(parameter)
		if err != nil {
			return
		}
	}
	marshalledOp = marshalledOperation{sc.key, marshalledParameters}
	return
}

func (m *marshalledOperation) unmarshal(unmarshal unmarshalFunc) (op operation, err error) {
	parameters := make([]interface{}, len(m.MarshalledParameters))
	for index, marshalledParameter := range m.MarshalledParameters {
		err = unmarshal(marshalledParameter, &parameters[index])
		if err != nil {
			return
		}
	}
	op = operation{m.Key, parameters}
	return
}
