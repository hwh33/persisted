package persisted

import (
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"os"
)

// Initialize the compaction threshold to 10 KB.
const initialCompactionThreshold = 10 * 1024

type action struct {
	key        string
	parameters []interface{}
}

type marshalFunc func(interface{}) ([]byte, error)
type unmarshalFunc func([]byte, interface{}) error

// The log type is used to persist data structures in this package. This is
// achieved by recording any actions which change the state of the structure.
type log struct {
	file                *os.File
	getCompactedActions func() []action
	compactThreshold    int64
	marshalFn           func(interface{}) ([]byte, error)
	unmarshalFn         func([]byte, interface{}) error
}

// Used for JSON encoding / decoding of actions.
type marshalledAction struct {
	Key                  string
	MarshalledParameters [][]byte
}

func newLog(filepath string, compactedActionsFn func() []action,
	marshalFn marshalFunc, unmarshalFn unmarshalFunc) (*log, error) {
	logFile, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	// TODO: check file
	return &log{
		logFile,
		compactedActionsFn,
		initialCompactionThreshold,
		marshalFn,
		unmarshalFn,
	}, nil
}

// Records the action in the log.
func (l *log) addAction(a action) error {
	ma, err := a.marshal(l.marshalFn)
	if err != nil {
		return err
	}
	err = json.NewEncoder(l.file).Encode(ma)
	if err != nil {
		return err
	}
	return l.compactIfNecessary()
}

func (l *log) applyActions(actionFunctions map[string]func(...interface{}) error) error {
	// Compact first to save ourselves some time once we start rebuilding.
	err := l.compact()
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(l.file)
	var ma marshalledAction
	for {
		err := decoder.Decode(&ma)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		a, err := ma.unmarshal(l.unmarshalFn)
		if err != nil {
			return err
		}
		actionFunction := actionFunctions[a.key]
		err = actionFunction(a.parameters)
		if err != nil {
			return errors.New("Error applying action: " + err.Error())
		}
	}
	return nil
}

// Compact by converting the log into a series of default action calls.
func (l *log) compact() error {
	tempFile, err := ioutil.TempFile("", "TempCompactionFile-"+l.file.Name())
	if err != nil {
		return nil
	}
	actions := l.getCompactedActions()
	encoder := json.NewEncoder(tempFile)
	for _, a := range actions {
		ma, err := a.marshal(l.marshalFn)
		if err != nil {
			return errors.New("Marshalling error during compaction: " + err.Error())
		}
		err = encoder.Encode(ma)
		if err != nil {
			return errors.New("Error during compaction: " + err.Error())
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

func (a *action) marshal(marshal marshalFunc) (ma marshalledAction, err error) {
	marshalledParameters := make([][]byte, len(a.parameters))
	for index, parameter := range a.parameters {
		marshalledParameters[index], err = marshal(parameter)
		if err != nil {
			return
		}
	}
	ma = marshalledAction{a.key, marshalledParameters}
	return
}

func (ma *marshalledAction) unmarshal(unmarshal unmarshalFunc) (a action, err error) {
	parameters := make([]interface{}, len(ma.MarshalledParameters))
	for index, marshalledParameter := range ma.MarshalledParameters {
		err = unmarshal(marshalledParameter, parameters[index])
		if err != nil {
			return
		}
	}
	a = action{ma.Key, parameters}
	return
}
