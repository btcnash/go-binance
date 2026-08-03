package futures

import (
	"encoding/json"
	"testing"
)

func acceptInt64(int64)           {}
func acceptStringPointer(*string) {}

func TestCancelAlgoOrderRespAlgoIDUsesInt64(t *testing.T) {
	acceptInt64(CancelAlgoOrderResp{}.AlgoId)
}

func TestCreateAlgoOrderRespIcebergQuantityUsesOptionalString(t *testing.T) {
	acceptStringPointer(CreateAlgoOrderResp{}.IcebergQuantity)
}

func TestCreateAlgoOrderRespIcebergQuantityDecimalString(t *testing.T) {
	var resp CreateAlgoOrderResp
	if err := json.Unmarshal([]byte(`{"icebergQuantity":"0.00100000"}`), &resp); err != nil {
		t.Fatalf("unmarshal decimal string: %v", err)
	}
	if resp.IcebergQuantity == nil {
		t.Fatal("expected icebergQuantity")
	}
	if got := *resp.IcebergQuantity; got != "0.00100000" {
		t.Fatalf("unexpected icebergQuantity: %q", got)
	}
}

func TestCreateAlgoOrderRespIcebergQuantityNull(t *testing.T) {
	var resp CreateAlgoOrderResp
	if err := json.Unmarshal([]byte(`{"icebergQuantity":null}`), &resp); err != nil {
		t.Fatalf("unmarshal null: %v", err)
	}
	if resp.IcebergQuantity != nil {
		t.Fatalf("expected nil, got %q", *resp.IcebergQuantity)
	}
}
