package presentationprobe

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	pathpkg "path"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

var forbiddenIdentifiers = map[string]bool{
	"CorpusRunEnvelope": true, "CompareCorpus": true, "VerifyCorpusEnvelope": true,
	"ExtractVerifiedUStar": true, "BuildDevelopmentArtifact": true,
	"SignCorpusEnvelope": true, "EncodeReportNDJSON": true,
	"ProjectionBytes": true, "ProjectionSHA256": true,
}

var forbiddenSelectors = map[string]bool{
	"os.Create": true, "os.WriteFile": true, "os.Mkdir": true, "os.MkdirAll": true,
	"os.OpenFile": true, "os.Remove": true, "os.RemoveAll": true, "os.Rename": true,
	"os.Symlink": true, "os.Truncate": true, "os.NewFile": true,
	"exec.Command": true, "exec.CommandContext": true, "exec.LookPath": true,
	"os.StartProcess": true, "syscall.Exec": true, "syscall.ForkExec": true,
}

var cliOSAllowlist = map[string]bool{
	"os.Args": true, "os.Stdout": true, "os.Stderr": true, "os.Exit": true, "os.ReadFile": true,
}

func inspectReducedSurface(filename string, source any, production bool) []string {
	files := token.NewFileSet()
	file, err := parser.ParseFile(files, filename, source, 0)
	if err != nil {
		return []string{err.Error()}
	}
	aliases := map[string]string{}
	issues := []string{}
	for _, spec := range file.Imports {
		importPath, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			issues = append(issues, err.Error())
			continue
		}
		local := pathpkg.Base(importPath)
		if spec.Name != nil {
			local = spec.Name.Name
			if local == "." {
				issues = append(issues, fmt.Sprintf("dot import is forbidden for %s", importPath))
			}
		}
		aliases[local] = importPath
		if production && (importPath == "os" || importPath == "os/exec" || importPath == "syscall" || importPath == "net" || strings.HasPrefix(importPath, "net/")) {
			issues = append(issues, fmt.Sprintf("forbidden production import %s", importPath))
		}
		if !production && (importPath == "os/exec" || importPath == "syscall" || importPath == "net" || strings.HasPrefix(importPath, "net/")) {
			issues = append(issues, fmt.Sprintf("forbidden CLI import %s", importPath))
		}
	}
	ast.Inspect(file, func(node ast.Node) bool {
		switch value := node.(type) {
		case *ast.Ident:
			if forbiddenIdentifiers[value.Name] {
				issues = append(issues, "forbidden reduced-C2 symbol "+value.Name)
			}
		case *ast.SelectorExpr:
			left, ok := value.X.(*ast.Ident)
			if !ok {
				return true
			}
			importPath := aliases[left.Name]
			selector := pathpkg.Base(importPath) + "." + value.Sel.Name
			if forbiddenSelectors[selector] {
				issues = append(issues, "forbidden reduced-C2 side effect "+selector)
			}
			if !production && importPath == "os" && !cliOSAllowlist[selector] {
				issues = append(issues, "CLI os selector is not read-only "+selector)
			}
		}
		return true
	})
	return issues
}

func TestReducedC2HasNoProofBuildExtractionOrLaunchSurface(t *testing.T) {
	paths, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	cli, err := filepath.Glob(filepath.Join("..", "..", "cmd", "stoarama-presentation-proof", "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	paths = append(paths, cli...)
	for _, filename := range paths {
		if strings.HasSuffix(filename, "_test.go") {
			continue
		}
		production := !strings.Contains(filename, filepath.Join("cmd", "stoarama-presentation-proof"))
		for _, issue := range inspectReducedSurface(filename, nil, production) {
			t.Errorf("%s in %s", issue, filename)
		}
	}
}

func TestReducedC2SurfaceGuardResolvesAliasedImports(t *testing.T) {
	source := `package main
import sneaky "os"
func write(path string, data []byte) error { return sneaky.WriteFile(path, data, 0600) }
`
	if issues := inspectReducedSurface("alias.go", source, false); len(issues) == 0 {
		t.Fatal("aliased filesystem write escaped reduced-C2 surface guard")
	}
}
