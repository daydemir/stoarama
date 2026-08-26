package joinedrecording

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestJoinedProductionMediaCommandsUseBoundedProcess(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range files {
		base := filepath.Base(path)
		if strings.HasSuffix(base, "_test.go") || strings.HasPrefix(base, "media_process_") {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		execAliases := map[string]bool{}
		osAliases := map[string]bool{}
		for _, imported := range parsed.Imports {
			importPath, err := strconv.Unquote(imported.Path.Value)
			if err != nil {
				continue
			}
			name := filepath.Base(importPath)
			if imported.Name != nil {
				name = imported.Name.Name
			}
			switch importPath {
			case "os/exec":
				execAliases[name] = true
			case "os":
				osAliases[name] = true
			}
		}
		ast.Inspect(parsed, func(node ast.Node) bool {
			switch typed := node.(type) {
			case *ast.CallExpr:
				selector, ok := typed.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				identifier, ok := selector.X.(*ast.Ident)
				if !ok {
					return true
				}
				if execAliases[identifier.Name] && (selector.Sel.Name == "Command" || selector.Sel.Name == "CommandContext") {
					t.Errorf("%s uses exec.%s outside the bounded media-process wrapper", path, selector.Sel.Name)
				}
				if osAliases[identifier.Name] && selector.Sel.Name == "StartProcess" {
					t.Errorf("%s uses os.StartProcess outside the bounded media-process wrapper", path)
				}
			case *ast.CompositeLit:
				selector, ok := typed.Type.(*ast.SelectorExpr)
				if !ok || selector.Sel.Name != "Cmd" {
					return true
				}
				identifier, ok := selector.X.(*ast.Ident)
				if ok && execAliases[identifier.Name] {
					t.Errorf("%s constructs exec.Cmd outside the bounded media-process wrapper", path)
				}
			}
			return true
		})
	}
}
