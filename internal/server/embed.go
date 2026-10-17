package server

import (
	"embed"
	"io/fs"
)

// dist is filled by `pnpm build` in _ui/ (vite outDir).
//
//go:embed all:dist
var distFS embed.FS

// UI returns the built frontend, or nil when only the placeholder is present.
func UI() (fs.FS, error) {
	sub, err := fs.Sub(distFS, "dist")
	if err != nil {
		return nil, err
	}

	if _, err := fs.Stat(sub, "index.html"); err != nil {
		return nil, nil
	}

	return sub, nil
}
