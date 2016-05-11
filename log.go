package persisted

import (
	"encoding/json"
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
// structure is obtained via an iterator returned by calling the getIter
// function. If the order in which the default action is applied matters, then
// the iterator must return the elements of the data structure in order.
//
// Encoding:
// Actions are recorded in the log file as JSON objects of the form:
// {"action":actionKey, "subject":subject.ToString(), "metadata":metadataJSON}
// where metadataJSON is the JSON encoding of the metadata interface{}.

// Initialize the compaction threshold to 10 KB.
const initialCompactionThreshold = 10 * 1024

type log struct {
	file             *os.File
	getIter          func() func() json.Marshaler
	actionFunctions  map[string]actionFunction
	defaultActionKey string
	compactThreshold int64
}

// A function which applies the action to the object with the given subject and
// metadata. In 'list.append(newElement)', list is the object and newElement is
// the subject. The metadata is nil. In 'list.remove(2)', list is the object and
// 2 is the metadata. The subject is nil.
type actionFunction func(
	object interface{}, parameters ...interface{})

// Used for JSON encoding / decoding of actions.
type action struct {
	key        string
	parameters []json.Marshaler
}

// If the file at filepath does not exist or is empty, a brand new log is made.
// This will create a new file, but the parent directory must exist. If the file
// is not empty, it will be interpreted as an existing log.
func newLog(filepath string, iterFunction func() func() json.Marshaler,
	actions map[string]actionFunction, defaultActionKey string) (*log, error) {

	// TODO: check input file

	logFile, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	return &log{
		logFile, iterFunction, actions, defaultActionKey, initialCompactionThreshold,
	}, nil
}

// Records the action in the log.
func (l *log) addAction(actionKey string, parameters ...json.Marshaler) error {
	err := writeAction(l.file, actionKey, parameters...)
	l.compactIfNecessary()
	return err
}

func (l *log) setCompactionThreshold(compactionThreshold int64) error {
	l.compactThreshold = compactionThreshold
	return l.compactIfNecessary()
}

// Runs through the log and applies every recorded action to toBuild. Compaction
// will be run before this method returns as initialization is a good time for
// cleanup.
func (l *log) buildFromLog(toBuild interface{}, unmarshalFn func([]byte, interface{}) error,
	newElementFn func() interface{}) error {
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
		err = decoder.Decode(decodedAction)
		if err != nil {
			return err
		}
		actionFn := l.actionFunctions[decodedAction.key]
		actionFn(toBuild, decodedAction.parameters)
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
		err = writeAction(tempFile, l.defaultActionKey, current)
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

func writeAction(file *os.File, actionKey string, parameters ...json.Marshaler) error {
	return json.NewEncoder(file).Encode(action{actionKey, parameters})
}
