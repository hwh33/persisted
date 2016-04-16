// Package persisted provides data structures which actively persist to disk.
package persisted

// Stringable types can marshal themselves into text, then later unmarshal
// themselves from that text.
type Stringable interface {
	ToString() (text string, err error)
	FromString(text string) error
}

// DecodeFunction is a function which constructs a Stringable from its
// string form.
type DecodeFunction func(string) (Stringable, error)
