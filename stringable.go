package persisted

// Stringable types can marshal themselves into text, then later unmarshal
// themselves from that text.
type Stringable interface {
	ToString() (text string, err error)
	FromString(text string) error
}
