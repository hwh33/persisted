// Package persisted provides data structures which actively persist to disk.
package persisted

// Stringable types can marshal themselves into text. They are unmarshalled by
// a DecodeFunction.
type Stringable interface {
	ToString() string
}

// DecodeFunction is a function which constructs a Stringable from its
// string form.
type DecodeFunction func(string) (Stringable, error)
