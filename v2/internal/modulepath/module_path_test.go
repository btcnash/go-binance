package modulepath

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

const canonicalModulePath = "github.com/btcnash/go-binance/v2"

func TestRepositoryUsesCanonicalModulePath(t *testing.T) {
	t.Helper()

	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve current test file")
	}

	moduleRoot := filepath.Clean(filepath.Join(filepath.Dir(currentFile), "..", ".."))
	repositoryRoot := filepath.Dir(moduleRoot)

	goMod, err := os.ReadFile(filepath.Join(moduleRoot, "go.mod"))
	if err != nil {
		t.Fatalf("read v2/go.mod: %v", err)
	}
	firstLine := strings.SplitN(string(goMod), "\n", 2)[0]
	if firstLine != "module "+canonicalModulePath {
		t.Fatalf("module declaration = %q, want %q", firstLine, "module "+canonicalModulePath)
	}

	legacyPath := "github.com/" + "adshao/go-binance"
	var offenders []string
	err = filepath.WalkDir(repositoryRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "vendor":
				return filepath.SkipDir
			}
			return nil
		}

		switch filepath.Ext(path) {
		case ".go", ".mod", ".md":
		default:
			return nil
		}

		content, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		if bytes.Contains(content, []byte(legacyPath)) {
			relative, relErr := filepath.Rel(repositoryRoot, path)
			if relErr != nil {
				relative = path
			}
			offenders = append(offenders, relative)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan repository: %v", err)
	}
	if len(offenders) != 0 {
		t.Fatalf("legacy module path remains in %d file(s):\n%s", len(offenders), strings.Join(offenders, "\n"))
	}
}
