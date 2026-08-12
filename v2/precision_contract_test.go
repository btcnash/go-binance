package binance_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
)

func TestPrecisionStaticContract(t *testing.T) {
	_, here, _, _ := runtime.Caller(0)
	root := filepath.Dir(here)
	allowedFloat64 := map[string]int{
		"common/websocket/managed/backoff.go": 3,
		"common/websocket/managed/types.go":   2,
		"futures/private/session.go":          2,
		"futures/private/types.go":            3,
	}
	seenFloat64 := map[string]int{}
	seenFloat64Calls := map[string]int{}
	var violations []string

	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if d.IsDir() {
			if rel == "vendor" || strings.HasPrefix(rel, "vendor/") || rel == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if err != nil {
			return err
		}
		ast.Inspect(file, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.Ident:
				if x.Name == "float32" {
					violations = append(violations, rel+":"+fset.Position(x.Pos()).String()+": float32 forbidden")
				}
				if x.Name == "float64" {
					seenFloat64[rel]++
					if _, ok := allowedFloat64[rel]; !ok {
						violations = append(violations, rel+":"+fset.Position(x.Pos()).String()+": business/protocol float64 forbidden")
					}
				}
			case *ast.CallExpr:
				if sel, ok := x.Fun.(*ast.SelectorExpr); ok {
					if sel.Sel.Name == "ParseFloat" {
						if id, ok := sel.X.(*ast.Ident); ok && id.Name == "strconv" {
							violations = append(violations, rel+":"+fset.Position(x.Pos()).String()+": strconv.ParseFloat forbidden")
						}
					}
					if sel.Sel.Name == "Float64" {
						seenFloat64Calls[rel]++
						if rel != "common/websocket/managed/backoff.go" {
							violations = append(violations, rel+":"+fset.Position(x.Pos()).String()+": Float64 conversion forbidden outside control jitter")
						}
					}
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	for file, want := range allowedFloat64 {
		if got := seenFloat64[file]; got != want {
			violations = append(violations, file+": control float64 whitelist count changed")
		}
	}
	if got := seenFloat64Calls["common/websocket/managed/backoff.go"]; got != 1 {
		violations = append(violations, "common/websocket/managed/backoff.go: control rng.Float64 whitelist count changed")
	}
	for file := range seenFloat64 {
		if _, ok := allowedFloat64[file]; !ok {
			continue
		}
	}

	// Known dynamic response/public-return regressions are banned even though request-side any remains allowed.
	banned := map[string][]string{
		"exchange_info_service.go":          {"ExchangeFilters []any", "Filters []map[string]any"},
		"futures/exchange_info_service.go":  {"ExchangeFilters []any", "Filters []map[string]any"},
		"delivery/exchange_info_service.go": {"ExchangeFilters []any", "Filters []map[string]any", "UnderlyingSubType []any"},
		"options/exchange_info_service.go":  {"Filters []map[string]any"},
		"delivery/websocket_service.go":     {"Time any `json:\"E\"`", "case float64:"},
		"options/order_service.go":          {"(res []any", "[]map[string]any"},
	}
	for rel, needles := range banned {
		data, err := fs.ReadFile(os.DirFS(root), filepath.FromSlash(rel))
		if err != nil {
			t.Fatal(err)
		}
		text := string(data)
		for _, needle := range needles {
			if strings.Contains(text, needle) {
				violations = append(violations, rel+": banned response pattern "+needle)
			}
		}
	}

	if len(violations) != 0 {
		sort.Strings(violations)
		t.Fatalf("precision contract violations:\n%s", strings.Join(violations, "\n"))
	}
}
