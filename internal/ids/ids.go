// Package ids makes identifiers: a type prefix and a UUIDv7, which sort by creation time,
// so an ID also serves as a stable pagination cursor.
package ids

import (
	"crypto/rand"
	"encoding/hex"

	"github.com/google/uuid"
)

// New returns a new identifier, such as ses_018f2c….
func New(prefix string) string {
	id, err := uuid.NewV7()
	if err != nil {
		// Only a failure of the system's random source gets here, which nothing survives.
		panic(err)
	}
	return prefix + "_" + hex.EncodeToString(id[:])
}

// Secret returns 256 random bits as text, for tokens and credentials.
func Secret() string {
	var b [32]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read never returns an error
	return hex.EncodeToString(b[:])
}
