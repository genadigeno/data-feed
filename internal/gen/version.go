// Package gen is the generation engine. It depends on no transport, storage or
// interface code.
package gen

// Version identifies the exact generator algorithms. Bump it on any change that
// alters output bytes for an existing snapshot: workers only serve sessions whose
// engine version they support.
const Version = 1
