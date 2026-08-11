package environmentapi

import (
	"testing"

	binance "github.com/btcnash/go-binance/v2"
)

func TestSetEnvironmentSwitchesSpotUSDMAndCOINMREST(t *testing.T) {
	if err := binance.SetEnvironment(binance.EnvironmentMainnet); err != nil {
		t.Fatal(err)
	}
	if got := binance.NewClient("", "").BaseURL; got != "https://api.binance.com" {
		t.Fatalf("spot mainnet BaseURL = %q", got)
	}
	if got := binance.NewFuturesClient("", "").BaseURL; got != "https://fapi.binance.com" {
		t.Fatalf("usdm mainnet BaseURL = %q", got)
	}
	if got := binance.NewDeliveryClient("", "").BaseURL; got != "https://dapi.binance.com" {
		t.Fatalf("coinm mainnet BaseURL = %q", got)
	}

	if err := binance.SetEnvironment(binance.EnvironmentTestnet); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = binance.SetEnvironment(binance.EnvironmentMainnet) })
	if got := binance.NewClient("", "").BaseURL; got != "https://testnet.binance.vision" {
		t.Fatalf("spot testnet BaseURL = %q", got)
	}
	if got := binance.NewFuturesClient("", "").BaseURL; got != "https://demo-fapi.binance.com" {
		t.Fatalf("usdm testnet BaseURL = %q", got)
	}
	if got := binance.NewDeliveryClient("", "").BaseURL; got != "https://demo-dapi.binance.com" {
		t.Fatalf("coinm testnet BaseURL = %q", got)
	}
}

func TestSetEnvironmentRejectsInvalidWithoutChangingSelection(t *testing.T) {
	if err := binance.SetEnvironment(binance.EnvironmentTestnet); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = binance.SetEnvironment(binance.EnvironmentMainnet) })
	if err := binance.SetEnvironment(binance.Environment(99)); err == nil {
		t.Fatal("SetEnvironment(invalid) error = nil")
	}
	if got := binance.NewFuturesClient("", "").BaseURL; got != "https://demo-fapi.binance.com" {
		t.Fatalf("environment changed after invalid SetEnvironment: %q", got)
	}
}

func TestSpotMainnetOnlyAnnouncementRejectsTestnet(t *testing.T) {
	if err := binance.SetEnvironment(binance.EnvironmentTestnet); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = binance.SetEnvironment(binance.EnvironmentMainnet) })
	_, _, err := binance.WsAnnouncementServe(binance.WsAnnouncementParam{}, nil, nil)
	if err == nil {
		t.Fatal("WsAnnouncementServe accepted testnet")
	}
}
