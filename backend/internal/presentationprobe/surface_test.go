package presentationprobe

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
)

func TestReducedC2HasNoProofBuildExtractionOrLaunchSurface(t *testing.T) {
	forbiddenIdentifiers := map[string]bool{
		"CorpusRunEnvelope": true, "CompareCorpus": true, "VerifyCorpusEnvelope": true,
		"ExtractVerifiedUStar": true, "BuildDevelopmentArtifact": true,
		"SignCorpusEnvelope": true, "EncodeReportNDJSON": true,
		"ProjectionBytes": true, "ProjectionSHA256": true,
	}
	forbiddenSelectors := map[string]bool{
		"os.Create": true, "os.WriteFile": true, "os.Mkdir": true, "os.MkdirAll": true,
		"exec.Command": true, "exec.CommandContext": true, "os.StartProcess": true,
		"syscall.Exec": true,
	}
	paths, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	cli, err := filepath.Glob(filepath.Join("..", "..", "cmd", "stoarama-presentation-proof", "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	paths = append(paths, cli...)
	files := token.NewFileSet()
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(files, path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			switch value := node.(type) {
			case *ast.Ident:
				if forbiddenIdentifiers[value.Name] {
					t.Errorf("forbidden reduced-C2 symbol %s in %s", value.Name, path)
				}
			case *ast.SelectorExpr:
				left, ok := value.X.(*ast.Ident)
				if ok && forbiddenSelectors[left.Name+"."+value.Sel.Name] {
					t.Errorf("forbidden reduced-C2 side effect %s.%s in %s", left.Name, value.Sel.Name, path)
				}
			}
			return true
		})
	}
}
