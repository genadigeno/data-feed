package protocol

import (
	"testing"

	"github.com/genadigeno/data-feed/internal/apperr"
)

func TestCheckCompatible(t *testing.T) {
	if err := CheckCompatible(Version); err != nil {
		t.Fatalf("same version rejected: %v", err)
	}
	err := CheckCompatible(Version + 1)
	e, ok := apperr.As(err)
	if !ok || e.Code != CodeVersionMismatch {
		t.Fatalf("other version: %v", err)
	}
}
