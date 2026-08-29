// Package db embeds the canonical migration set for the service migrator.
package db

import "embed"

//go:embed migrations/*.sql
var MigrationsFS embed.FS
