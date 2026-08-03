package futures

import (
	"context"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestStrongOrderIDWireContract(t *testing.T) {
	for _, test := range []struct {
		name string
		raw  string
		new  func() any
	}{
		{name: "algo zero", raw: `0`, new: func() any { return new(AlgoOrderID) }},
		{name: "algo negative", raw: `-1`, new: func() any { return new(AlgoOrderID) }},
		{name: "algo string", raw: `"1"`, new: func() any { return new(AlgoOrderID) }},
		{name: "order zero", raw: `0`, new: func() any { return new(OrderID) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := json.Unmarshal([]byte(test.raw), test.new()); err == nil {
				t.Fatalf("Unmarshal(%s) error = nil", test.raw)
			}
		})
	}

	var algoID AlgoOrderID
	if err := json.Unmarshal([]byte(`9223372036854775807`), &algoID); err != nil {
		t.Fatalf("Unmarshal(max algoId) error = %v", err)
	}
	if algoID != AlgoOrderID(9223372036854775807) {
		t.Fatalf("algoID = %d", algoID)
	}
	raw, err := json.Marshal(algoID)
	if err != nil {
		t.Fatalf("Marshal(algoID) error = %v", err)
	}
	if string(raw) != `9223372036854775807` {
		t.Fatalf("Marshal(algoID) = %s", raw)
	}
}

func TestOptionalOrderIDWireContract(t *testing.T) {
	for _, raw := range []string{`null`, `""`} {
		var value OptionalOrderID
		if err := json.Unmarshal([]byte(raw), &value); err != nil {
			t.Fatalf("Unmarshal(%s) error = %v", raw, err)
		}
		if value.Valid {
			t.Fatalf("Unmarshal(%s) Valid = true", raw)
		}
	}

	const largeID OrderID = 9223372036854775807
	var value OptionalOrderID
	if err := json.Unmarshal([]byte(`"9223372036854775807"`), &value); err != nil {
		t.Fatalf("Unmarshal(large ID) error = %v", err)
	}
	if !value.Valid || value.Value != largeID {
		t.Fatalf("value = %#v", value)
	}

	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	if got := string(raw); got != `"9223372036854775807"` {
		t.Fatalf("Marshal() = %s", got)
	}

	for _, raw := range []string{`"0"`, `"-1"`, `"abc"`, `1`} {
		var invalid OptionalOrderID
		if err := json.Unmarshal([]byte(raw), &invalid); err == nil {
			t.Fatalf("Unmarshal(%s) error = nil", raw)
		}
	}
}

func TestAlgoRESTResponseFixtureUsesTypedValues(t *testing.T) {
	payload := []byte(`{
		"algoId":9223372036854775807,
		"clientAlgoId":"client-1",
		"algoType":"CONDITIONAL",
		"orderType":"STOP",
		"symbol":"BTCUSDT",
		"side":"BUY",
		"positionSide":"BOTH",
		"timeInForce":"GTC",
		"quantity":"0.00100000000000000001",
		"algoStatus":"TRIGGERED",
		"actualOrderId":"9007199254740993",
		"actualPrice":"85000.10000000",
		"actualQty":"0.00050000000000000001",
		"executedQty":"0.00040000000000000001",
		"avgPrice":"85001.123456789012345678",
		"triggerPrice":"85000.10",
		"price":"85010.20",
		"icebergQuantity":"0.00010000000000000001",
		"tpTriggerPrice":"90000.10",
		"tpPrice":"90001.10",
		"slTriggerPrice":"80000.10",
		"slPrice":"79999.10",
		"activatePrice":"84000.10",
		"callbackRate":"0.1"
	}`)

	var response GetAlgoOrderResp
	if err := json.Unmarshal(payload, &response); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if response.AlgoId != AlgoOrderID(9223372036854775807) {
		t.Fatalf("AlgoId = %v", response.AlgoId)
	}
	if !response.ActualOrderId.Valid || response.ActualOrderId.Value != OrderID(9007199254740993) {
		t.Fatalf("ActualOrderId = %#v", response.ActualOrderId)
	}
	assertDecimalString(t, "Quantity", response.Quantity, "0.00100000000000000001")
	assertOptionalDecimalString(t, "ActualPrice", response.ActualPrice, "85000.1")
	assertOptionalDecimalString(t, "ActualQty", response.ActualQty, "0.00050000000000000001")
	assertOptionalDecimalString(t, "ExecutedQty", response.ExecutedQty, "0.00040000000000000001")
	assertOptionalDecimalString(t, "AvgPrice", response.AvgPrice, "85001.123456789012345678")
	assertOptionalDecimalString(t, "IcebergQuantity", response.IcebergQuantity, "0.00010000000000000001")
	assertOptionalDecimalString(t, "TpPrice", response.TpPrice, "90001.1")
	assertOptionalDecimalString(t, "ActivatePrice", response.ActivatePrice, "84000.1")
	assertOptionalDecimalString(t, "CallbackRate", response.CallbackRate, "0.1")
}

func TestAlgoRESTResponseEmptyOptionalValues(t *testing.T) {
	payload := []byte(`{
		"algoId":123,
		"quantity":"1",
		"actualOrderId":"",
		"actualPrice":"",
		"actualQty":null,
		"executedQty":"0",
		"avgPrice":"",
		"triggerPrice":"0",
		"price":"0",
		"icebergQuantity":null,
		"tpTriggerPrice":"",
		"tpPrice":"",
		"slTriggerPrice":"",
		"slPrice":"",
		"activatePrice":"",
		"callbackRate":""
	}`)
	var response GetAlgoOrderResp
	if err := json.Unmarshal(payload, &response); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if response.ActualOrderId.Valid || response.ActualPrice.Valid || response.ActualQty.Valid || response.AvgPrice.Valid || response.IcebergQuantity.Valid {
		t.Fatalf("optional values not empty: %#v", response)
	}
	if !response.ExecutedQty.Valid || !response.ExecutedQty.Value.IsZero() {
		t.Fatalf("ExecutedQty = %#v", response.ExecutedQty)
	}
}

func TestAlgoRESTServiceEncodesDecimalAndDecodesTypedResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/fapi/v1/algoOrder" {
			t.Errorf("path = %q", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodPost {
			t.Errorf("method = %q", r.Method)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if err := r.ParseForm(); err != nil {
			t.Errorf("ParseForm() error = %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		for key, want := range map[string]string{
			"quantity":     "0.00100000000000000001",
			"price":        "85010.2",
			"triggerPrice": "85000.1",
		} {
			if got := r.Form.Get(key); got != want {
				t.Errorf("form[%s] = %q, want %q", key, got, want)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"algoId":9223372036854775807,
			"clientAlgoId":"client-1",
			"algoType":"CONDITIONAL",
			"orderType":"STOP",
			"symbol":"BTCUSDT",
			"side":"BUY",
			"positionSide":"BOTH",
			"timeInForce":"GTC",
			"quantity":"0.00100000000000000001",
			"algoStatus":"NEW",
			"triggerPrice":"85000.10",
			"price":"85010.20",
			"icebergQuantity":"0.00010000000000000001",
			"activatePrice":"",
			"callbackRate":null
		}`))
	}))
	defer server.Close()

	client := NewClient("key", "secret").SetApiEndpoint(server.URL)
	response, err := client.NewCreateAlgoOrderService().
		Symbol("BTCUSDT").
		Side(SideTypeBuy).
		Type(AlgoOrderTypeStop).
		Quantity(MustParseDecimal("0.00100000000000000001")).
		Price(MustParseDecimal("85010.20")).
		TriggerPrice(MustParseDecimal("85000.10")).
		Do(context.Background())
	if err != nil {
		t.Fatalf("Do() error = %v", err)
	}
	if response.AlgoId != AlgoOrderID(9223372036854775807) {
		t.Fatalf("AlgoId = %d", response.AlgoId)
	}
	assertDecimalString(t, "Quantity", response.Quantity, "0.00100000000000000001")
	assertOptionalDecimalString(t, "IcebergQuantity", response.IcebergQuantity, "0.00010000000000000001")
	if response.ActivatePrice.Valid || response.CallbackRate.Valid {
		t.Fatalf("empty optionals = %#v %#v", response.ActivatePrice, response.CallbackRate)
	}
}

func TestAlgoRESTServicesRejectNonPositiveIDsBeforeTransport(t *testing.T) {
	client := NewClient("key", "secret")
	client.do = func(*http.Request) (*http.Response, error) {
		t.Fatal("transport called for invalid ID")
		return nil, nil
	}
	for name, call := range map[string]func() error{
		"cancel": func() error {
			_, err := client.NewCancelAlgoOrderService().AlgoID(0).Do(context.Background())
			return err
		},
		"get": func() error {
			_, err := client.NewGetAlgoOrderService().AlgoID(-1).Do(context.Background())
			return err
		},
		"list open": func() error {
			_, err := client.NewListOpenAlgoOrdersService().AlgoID(0).Do(context.Background())
			return err
		},
		"list all": func() error {
			_, err := client.NewListAllAlgoOrdersService().Symbol("BTCUSDT").AlgoID(-1).Do(context.Background())
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := call(); err == nil {
				t.Fatal("error = nil")
			}
		})
	}
}

func TestAlgoRequestSettersAcceptDecimalAndEmitStrings(t *testing.T) {
	quantity := MustParseDecimal("0.00100000")
	price := MustParseDecimal("60000.1000")
	trigger := MustParseDecimal("59000.10")
	callback := MustParseDecimal("0.10")

	rest := newCreateAlgoOrderService(nil).
		Quantity(quantity).
		Price(price).
		TriggerPrice(trigger).
		CallbackRate(callback)
	if got := rest.param["quantity"]; got != "0.001" {
		t.Fatalf("REST quantity = %#v", got)
	}
	if got := rest.param["price"]; got != "60000.1" {
		t.Fatalf("REST price = %#v", got)
	}

	ws := NewAlgoOrderPlaceWsRequest().
		Symbol("BTCUSDT").
		Side(SideTypeBuy).
		Type(AlgoOrderTypeStop).
		Quantity(quantity).
		Price(price).
		TriggerPrice(trigger)
	params := ws.GetParams()
	if got := params["quantity"]; got != "0.001" {
		t.Fatalf("WS quantity = %#v", got)
	}
	if got := params["triggerPrice"]; got != "59000.1" {
		t.Fatalf("WS triggerPrice = %#v", got)
	}

	raw, err := json.Marshal(params)
	if err != nil {
		t.Fatalf("Marshal(params) error = %v", err)
	}
	if strings.Contains(string(raw), `"quantity":0.001`) {
		t.Fatalf("quantity encoded as JSON number: %s", raw)
	}
}

func TestAlgoCallbackRateUsesExactDecimalComparison(t *testing.T) {
	for _, raw := range []string{"0.1", "10"} {
		req := NewAlgoOrderPlaceWsRequest().
			Symbol("BTCUSDT").
			Side(SideTypeSell).
			Type(AlgoOrderTypeTrailingStopMarket).
			Quantity(MustParseDecimal("0.001")).
			CallbackRate(MustParseDecimal(raw))
		if err := req.Validate(); err != nil {
			t.Fatalf("callbackRate %s Validate() error = %v", raw, err)
		}
	}
	for _, raw := range []string{"0.09999999999999999999", "10.00000000000000000001"} {
		req := NewAlgoOrderPlaceWsRequest().
			Symbol("BTCUSDT").
			Side(SideTypeSell).
			Type(AlgoOrderTypeTrailingStopMarket).
			Quantity(MustParseDecimal("0.001")).
			CallbackRate(MustParseDecimal(raw))
		if err := req.Validate(); err == nil {
			t.Fatalf("callbackRate %s Validate() error = nil", raw)
		}
	}
}

func TestAlgoPublicModelsHaveNoPrimitiveFinancialTypes(t *testing.T) {
	financialNames := map[string]struct{}{
		"quantity": {}, "price": {}, "triggerprice": {}, "activateprice": {}, "activationprice": {},
		"callbackrate": {}, "actualprice": {}, "actualqty": {}, "executedqty": {}, "avgprice": {},
		"tptriggerprice": {}, "tpprice": {}, "sltriggerprice": {}, "slprice": {}, "icebergquantity": {},
	}
	files, err := filepath.Glob("algo_order*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	for _, filename := range files {
		if strings.HasSuffix(filename, "_test.go") {
			continue
		}
		source, err := os.ReadFile(filename)
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(fset, filename, source, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			switch value := node.(type) {
			case *ast.Field:
				for _, name := range value.Names {
					normalized := strings.ToLower(name.Name)
					if _, ok := financialNames[normalized]; ok && forbiddenPrimitive(value.Type) {
						t.Errorf("%s: %s uses forbidden primitive %s", filename, name.Name, exprText(value.Type))
					}
					if (normalized == "algoid" || normalized == "orderid") && isBareInt(value.Type) {
						t.Errorf("%s: %s uses int instead of a strong ID", filename, name.Name)
					}
					if normalized == "actualorderid" && forbiddenPrimitive(value.Type) {
						t.Errorf("%s: ActualOrderId must be OptionalOrderID", filename)
					}
				}
			case *ast.CallExpr:
				if selector, ok := value.Fun.(*ast.SelectorExpr); ok && (selector.Sel.Name == "ParseFloat" || selector.Sel.Name == "NewFromFloat") {
					t.Errorf("%s: decimal path calls %s", filename, selector.Sel.Name)
				}
			}
			return true
		})
	}
}

func assertDecimalString(t *testing.T, name string, value Decimal, want string) {
	t.Helper()
	if got := value.CanonicalString(); got != want {
		t.Fatalf("%s = %q, want %q", name, got, want)
	}
}

func assertOptionalDecimalString(t *testing.T, name string, value OptionalDecimal, want string) {
	t.Helper()
	if !value.Valid {
		t.Fatalf("%s.Valid = false", name)
	}
	assertDecimalString(t, name, value.Value, want)
}

func forbiddenPrimitive(expr ast.Expr) bool {
	if pointer, ok := expr.(*ast.StarExpr); ok {
		return forbiddenPrimitive(pointer.X)
	}
	identifier, ok := expr.(*ast.Ident)
	if !ok {
		return false
	}
	switch identifier.Name {
	case "string", "float32", "float64", "int", "int8", "int16", "int32", "int64", "uint", "uint8", "uint16", "uint32", "uint64":
		return true
	default:
		return false
	}
}

func isBareInt(expr ast.Expr) bool {
	identifier, ok := expr.(*ast.Ident)
	return ok && identifier.Name == "int"
}

func exprText(expr ast.Expr) string {
	return reflect.TypeOf(expr).String()
}
