package web

import "embed"

//go:embed *.html dashboard.js
var assets embed.FS

func ReadHTML(name string) ([]byte, error) {
	return assets.ReadFile(name)
}

func ReadStatic(name string) ([]byte, error) {
	return assets.ReadFile(name)
}
