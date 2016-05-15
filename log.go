package persisted

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
)

// The log type is used to persist data structures in this package. This is
// achieved by recording any actions which change the state of the structure.

// --- Implementation details ---
//
// Compaction:
// If the size of the log file (in bytes) grows larger than
// log.compactThreshold, it is compacted. To do so, we reduce the list of
// actions to a series of calls to a default function. For a list, this would
// be 'append'; for a hash table, this would be 'put'. If, after compaction, the
// log file is still larger than the threshold, we double the threshold to avoid
// thrashing.
//
// Obtaining the Current State:
// To re-write the log during compaction, the current state of the data
// structure is obtained via an iterator (obtained by calling the getIter
// function). The default action is applied, in order, to each element returned
// by the iterator.
//
// The Default Action:
// To resolve compaction with the iterator function, the default action must
// take only one parameter and this must be the parameter returned by the
// iterator.
//
// Encoding:
// Actions are recorded in the log file as JSON objects. The parameters to an
// action (passed in to the addAction method) will be encoded as JSON objects as
// well. By default, this is done with the json.Marshal function. The parameters
// are then read back out of the log using the json.Unmarshal function. This
// could result in undesired behavior like unexported fields of structs being
// set to their zero values. For a full description of the behavior of the json
// marshalling functions, see the godoc at golang.org/pkg/encoding/json. If you
// need to avoid this behavior, you can pass in custom marshalling functions
// using openCustomLog. The only requirements (besides the function signatures)
// are that the marshal function create a valid JSON object and the unmarshal
// function be able to read the produced JSON.

// Initialize the compaction threshold to 10 KB.
const initialCompactionThreshold = 10 * 1024

// A function which applies the action to the object with the given parameters.
type actionFunction func(object interface{}, parameters ...interface{})

type log struct {
	file             *os.File
	getIter          func() func() interface{}
	actionFunctions  map[string]actionFunction
	defaultActionKey string
	compactThreshold int64
	marshalFn        func(interface{}) ([]byte, error)
	unmarshalFn      func([]byte, interface{}) error
}

// Used for JSON encoding / decoding of actions.
type action struct {
	key                  string
	marshalledParameters [][]byte
}

// If the file at filepath does not exist or is empty, a brand new log is made.
// This will create a new file, but the parent directory must exist. If the file
// is not empty, it will be interpreted as an existing log.
func openLog(filepath string, iterFunction func() func() interface{},
	actions map[string]actionFunction, defaultActionKey string,
	marshalFn func(interface{}) ([]byte, error),
	unmarshalFn func([]byte, interface{}) error) (*log, error) {

	// TODO: check input file

	logFile, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	openedLog := &log{
		logFile,
		iterFunction,
		actions,
		defaultActionKey,
		initialCompactionThreshold,
		marshalFn,
		unmarshalFn,
	}
	err = openedLog.compact()
	if err != nil {
		return nil, err
	}
	fileInfo, err := openedLog.file.Stat()
	if err != nil {
		return nil, err
	}
	if openedLog.compactThreshold < fileInfo.Size() {
		openedLog.compactThreshold = fileInfo.Size() * 2
	}
	return openedLog, nil
}

// Records the action in the log.
func (l *log) addAction(actionKey string, parameters ...interface{}) error {
	var err error
	marshalledParameters := make([][]byte, len(parameters))
	for index, parameter := range parameters {
		marshalledParameters[index], err = l.marshalFn(parameter)
		if err != nil {
			return errors.New("Error marshalling action parameter: " + err.Error())
		}
	}
	err = writeAction(l.file, actionKey, marshalledParameters...)
	if err != nil {
		return err
	}
	return l.compactIfNecessary()
}

// Runs through the log and applies every recorded action to toBuild. Compaction
// will be run before this method returns as initialization is a good time for
// cleanup.
func (l *log) buildFromLog(toBuild interface{}) error {
	// Compact first to save ourselves some time once we start rebuilding.
	err := l.compact()
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(l.file)
	_, err = decoder.Token()
	if err != nil {
		return err
	}

	for decoder.More() {
		decodedAction := new(action)
		// TODO: make sure this works with custom marshal functions
		parameters := make([]interface{}, len(decodedAction.marshalledParameters))
		for index, marshalled := range decodedAction.marshalledParameters {
			err = l.unmarshalFn(marshalled, parameters[index])
			if err != nil {
				return errors.New("Error unmarshalling element: " + err.Error())
			}
		}
		actionFn := l.actionFunctions[decodedAction.key]
		actionFn(toBuild, parameters)
	}
	return nil
}

// Compact by converting the log into a series of default action calls.
func (l *log) compact() error {
	tempFile, err := ioutil.TempFile("", "TempCompactionFile-"+l.file.Name())
	if err != nil {
		return nil
	}
	iter := l.getIter()
	for current := iter(); current != nil; current = iter() {
		marshalledElement, err := l.marshalFn(current)
		if err != nil {
			return errors.New("Error marshalling element during compaction: " + err.Error())
		}
		err = writeAction(tempFile, l.defaultActionKey, marshalledElement)
		if err != nil {
			return err
		}
	}
	// If all went well, we can now over-write the existing log.
	return os.Rename(tempFile.Name(), l.file.Name())
}

// Compact if size(log) > compactionThreshold, otherwise no-op.
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
	if stat.Size() > l.compactThreshold {
		l.compactThreshold = l.compactThreshold * 2
	}
	return nil
}

func writeAction(file *os.File, actionKey string, marshalledParameters ...[]byte) error {
	return json.NewEncoder(file).Encode(action{actionKey, marshalledParameters})
}
