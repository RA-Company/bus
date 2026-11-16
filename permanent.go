package bus

import "errors"

// permanentError marks an error returned by a receiver as not worth retrying.
type permanentError struct {
	err error
}

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// Permanent wraps err to tell the bus that retrying the message cannot help (for example the
// payload is semantically invalid). A receiver that returns such an error from Execute is not
// retried: the message goes straight to the dead-letter stream with reason "permanent" and is
// acknowledged. The wrapped error is still reachable through errors.Is / errors.As.
//
// Permanent(nil) returns nil.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsPermanent reports whether err, or any error it wraps, was marked with Permanent.
func IsPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}
