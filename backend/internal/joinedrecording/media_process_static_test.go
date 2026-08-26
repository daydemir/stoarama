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
		for _, imported := range parsed.Imports {
			importPath, err := strconv.Unquote(imported.Path.Value)
			if err != nil || importPath != "os/exec" {
				continue
			}
			name := "exec"
			if imported.Name != nil {
				name = imported.Name.Name
			}
			execAliases[name] = true
		}
		ast.Inspect(parsed, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || (selector.Sel.Name != "Command" && selector.Sel.Name != "CommandContext") {
				return true
			}
			identifier, ok := selector.X.(*ast.Ident)
			if ok && execAliases[identifier.Name] {
				t.Errorf("%s uses exec.%s outside the bounded media-process wrapper", path, selector.Sel.Name)
			}
			return true
		})
	}
}
