package persisted

import (
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"os"
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

// stateChange represents some operation which changes the state of a persisted
// data structure.
type stateChange struct {
	key        string
	parameters []interface{}
}

// Used to marshal and unmarshal the parameters in a stateChange.
type marshalFunc func(interface{}) ([]byte, error)
type unmarshalFunc func([]byte, interface{}) error

// The log type is used to persist data structures in this package. This is
// achieved by recording any operations which change the state of the structure.
type log struct {
	file                *os.File
	getCompactedChanges func() []stateChange
	compactThreshold    int64
	marshaler           marshalFunc
	unmarshaler         unmarshalFunc
}

// Used for JSON encoding / decoding of state changes.
type marshalledStateChange struct {
	Key                  string
	MarshalledParameters [][]byte
}

// Initializes a log backed by the file at the provided file path. If this file
// already exists, it will be interpreted as an existing log. If the file does
// not exist, it will be created, but all parent directories must exist.
//
// compactedChangesFn should return the most compact series of state changes
// which represent the data structure recorded in this log. This should be a
// closure so that it always returns a set of changes reflecting the current
// state.
//
// The marshal and unmarshal functions are used for parameters passed in to the
// add method. These methods must produce valid JSON and a "round-tripped"
// parameter (one which has been marshalled, then unmarshalled) must be
// equivalent to its original self.
func newLog(filepath string, compactedChangesFn func() []stateChange,
	marshalFn marshalFunc, unmarshalFn unmarshalFunc) (*log, error) {
	logFile, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	// TODO: check file
	return &log{
		logFile,
		compactedChangesFn,
		initialCompactionThreshold,
		marshalFn,
		unmarshalFn,
	}, nil
}

// Records the state change in the log.
func (l *log) add(change stateChange) error {
	marshalledChange, err := change.marshal(l.marshaler)
	if err != nil {
		return err
	}
	err = json.NewEncoder(l.file).Encode(marshalledChange)
	if err != nil {
		return err
	}
	return l.compactIfNecessary()
}

// Uses the input map to replay every state change in the log. This method will
// iterate through the changes recorded in the log and unmarshal the associated
// parameters. The associated function will be looked up in the input map and
// called with the recorded parameters.
// The values in the inut map should most likely be closures so that, when
// applied, they have the desired effect on the state of the data structure
// backed by this log.
func (l *log) replay(stateChangeFunctions map[string]func(...interface{}) error) error {
	decoder := json.NewDecoder(l.file)
	var marshalledChange marshalledStateChange
	for {
		err := decoder.Decode(&marshalledChange)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		change, err := marshalledChange.unmarshal(l.unmarshaler)
		if err != nil {
			return err
		}
		stateChangeFunction, keyExists := stateChangeFunctions[change.key]
		if !keyExists {
			return errors.New("Recorded key <" + change.key + "> not found in input map")
		}
		err = stateChangeFunction(change.parameters)
		if err != nil {
			return errors.New("Error applying state change: " + err.Error())
		}
	}
	// Compact now as we'd rather take a performance hit during initialization.
	return l.compact()
}

// Compact the log. This is equivalent to calling l.add, in order, for every
// state change returned by l.getCompactedChanges().
func (l *log) compact() error {
	tempFile, err := ioutil.TempFile("", "TempCompactionFile-"+l.file.Name())
	if err != nil {
		return nil
	}
	changes := l.getCompactedChanges()
	encoder := json.NewEncoder(tempFile)
	for _, change := range changes {
		marshalledChange, err := change.marshal(l.marshaler)
		if err != nil {
			return errors.New("Marshalling error during compaction: " + err.Error())
		}
		err = encoder.Encode(marshalledChange)
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
		err := l.compact()
		if err != nil {
			return err
		}
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
	return nil
}

func (sc *stateChange) marshal(marshal marshalFunc) (m marshalledStateChange, err error) {
	marshalledParameters := make([][]byte, len(sc.parameters))
	for index, parameter := range sc.parameters {
		marshalledParameters[index], err = marshal(parameter)
		if err != nil {
			return
		}
	}
	m = marshalledStateChange{sc.key, marshalledParameters}
	return
}

func (m *marshalledStateChange) unmarshal(unmarshal unmarshalFunc) (sc stateChange, err error) {
	parameters := make([]interface{}, len(m.MarshalledParameters))
	for index, marshalledParameter := range m.MarshalledParameters {
		err = unmarshal(marshalledParameter, parameters[index])
		if err != nil {
			return
		}
	}
	sc = stateChange{m.Key, parameters}
	return
}
