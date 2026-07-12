package common

import (
	"encoding/json"
	"testing"
)

func TestPriceLevelUnmarshalBinanceArray(t *testing.T) {
	var level PriceLevel
	if err := json.Unmarshal([]byte(`["46248.03","0.000"]`), &level); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if level.Price != "46248.03" || level.Quantity != "0.000" {
		t.Fatalf("level = %#v", level)
	}
}

func TestPriceLevelUnmarshalPreservesObjectCompatibility(t *testing.T) {
	var level PriceLevel
	if err := json.Unmarshal([]byte(`{"Price":"1.25","Quantity":"3.5"}`), &level); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if level.Price != "1.25" || level.Quantity != "3.5" {
		t.Fatalf("level = %#v", level)
	}
}

func TestPriceLevelUnmarshalRejectsShortArray(t *testing.T) {
	var level PriceLevel
	if err := json.Unmarshal([]byte(`["46248.03"]`), &level); err == nil {
		t.Fatal("Unmarshal() error = nil, want malformed array error")
	}
}
