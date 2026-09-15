// Package db exposes the versioned SQL migrations as an embedded filesystem so
// the Go binaries can apply them without a separate migration tool.
package db

import "embed"

// Migrations contains migrations/NNNN_name.sql files, applied in lexical order.
//
//go:embed migrations/*.sql
var Migrations embed.FS
