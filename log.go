package persisted

import (
	"os"
)

type log struct {
	file             *os.File
	iterFunction     func() func() Stringable
	actions          map[string]func(interface{}, Stringable)
	compactThreshold uint32
}

func newLog(filepath string, iterFunction func() func() Stringable,
	actions map[string]func(Stringable)) (*log, error) {
	// TODO: implement me!
	return nil, nil
}

func (l *log) addAction(actionKey string, subject Stringable) {
	// TODO: implement me!
}

func (l *log) setCompactionThreshold(compactionThreshold uint32) {
	// TODO: implement me!
}

func (l *log) buildFromLog(toBuild interface{}) {
	// TODO: implement me!
}

func (l *log) compact() {
	// TODO: implement me!
}
