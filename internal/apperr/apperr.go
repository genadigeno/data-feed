// Package apperr defines the error type used at application boundaries.
//
// Engine packages return plain errors; application services translate them into
// *Error, which interfaces (HTTP, CLI, worker protocol) map to status codes.
package apperr

import (
	"errors"
	"fmt"
	"net/http"
)

// Kind classifies an error for mapping to transport-level responses.
type Kind uint8

const (
	Internal Kind = iota
	InvalidArgument
	NotFound
	Conflict
	PreconditionFailed
	Unauthenticated
	PermissionDenied
	ResourceExhausted
	Unavailable
	Canceled
)

// CodeInternal is the only code clients see for Internal errors.
const CodeInternal = "ERR_INTERNAL"

// Problem is one field-level validation issue.
type Problem struct {
	Path    string `json:"path"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (p Problem) String() string {
	if p.Path == "" {
		return fmt.Sprintf("%s: %s", p.Code, p.Message)
	}
	return fmt.Sprintf("%s: %s: %s", p.Path, p.Code, p.Message)
}

// Error is an application error. Message, Problems and Details are safe for
// clients; Cause is internal and never serialised.
type Error struct {
	Kind      Kind
	Code      string
	Message   string
	Problems  []Problem
	Details   map[string]any
	Retryable bool
	Cause     error
}

func (e *Error) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.Cause)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *Error) Unwrap() error { return e.Cause }

// New returns an error of the given kind.
func New(kind Kind, code, msg string) *Error {
	return &Error{Kind: kind, Code: code, Message: msg, Retryable: kind == Unavailable}
}

// Wrap returns an error of the given kind with cause attached.
func Wrap(cause error, kind Kind, code, msg string) *Error {
	e := New(kind, code, msg)
	e.Cause = cause
	return e
}

// Validation returns an InvalidArgument error carrying problems.
func Validation(code, msg string, problems []Problem) *Error {
	e := New(InvalidArgument, code, msg)
	e.Problems = problems
	return e
}

// As extracts an *Error from err's chain.
func As(err error) (*Error, bool) {
	var e *Error
	if errors.As(err, &e) {
		return e, true
	}
	return nil, false
}

// HTTPStatus maps a kind to its HTTP status code. Validation problems use 422
// through HTTPStatusFor; plain InvalidArgument means a malformed request.
func HTTPStatus(k Kind) int {
	switch k {
	case InvalidArgument:
		return http.StatusBadRequest
	case NotFound:
		return http.StatusNotFound
	case Conflict:
		return http.StatusConflict
	case PreconditionFailed:
		return http.StatusPreconditionFailed
	case Unauthenticated:
		return http.StatusUnauthorized
	case PermissionDenied:
		return http.StatusForbidden
	case ResourceExhausted:
		return http.StatusTooManyRequests
	case Unavailable:
		return http.StatusServiceUnavailable
	case Canceled:
		return 499
	default:
		return http.StatusInternalServerError
	}
}

// HTTPStatusFor returns the status for a concrete error.
func HTTPStatusFor(e *Error) int {
	if e.Kind == InvalidArgument && len(e.Problems) > 0 {
		return http.StatusUnprocessableEntity
	}
	return HTTPStatus(e.Kind)
}

// CLI exit codes.
const (
	ExitOK          = 0
	ExitGeneric     = 1
	ExitUsage       = 2
	ExitNotFound    = 3
	ExitConflict    = 4
	ExitAuth        = 5
	ExitUnavailable = 6
	ExitInterrupted = 130
)

// ExitCode maps a kind to its CLI exit code.
func ExitCode(k Kind) int {
	switch k {
	case InvalidArgument:
		return ExitUsage
	case NotFound:
		return ExitNotFound
	case Conflict, PreconditionFailed:
		return ExitConflict
	case Unauthenticated, PermissionDenied:
		return ExitAuth
	case Unavailable:
		return ExitUnavailable
	case Canceled:
		return ExitInterrupted
	default:
		return ExitGeneric
	}
}
