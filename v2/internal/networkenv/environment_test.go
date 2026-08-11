package networkenv

import (
	"sync"
	"testing"
)

func TestEnvironmentDefaultAndSet(t *testing.T) {
	if err := Set(Mainnet); err != nil {
		t.Fatal(err)
	}
	if got := Current(); got != Mainnet {
		t.Fatalf("default/current environment = %v, want %v", got, Mainnet)
	}
	if err := Set(Testnet); err != nil {
		t.Fatal(err)
	}
	if got := Current(); got != Testnet {
		t.Fatalf("current environment = %v, want %v", got, Testnet)
	}
	if err := Set(Mainnet); err != nil {
		t.Fatal(err)
	}
}

func TestEnvironmentRejectsInvalidWithoutChangingState(t *testing.T) {
	if err := Set(Testnet); err != nil {
		t.Fatal(err)
	}
	if err := Set(Environment(99)); err == nil {
		t.Fatal("Set(invalid) error = nil")
	}
	if got := Current(); got != Testnet {
		t.Fatalf("current environment = %v, want %v", got, Testnet)
	}
	if err := Set(Mainnet); err != nil {
		t.Fatal(err)
	}
}

func TestEnvironmentConcurrentAccess(t *testing.T) {
	if err := Set(Mainnet); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				if i%2 == 0 {
					_ = Set(Mainnet)
				} else {
					_ = Set(Testnet)
				}
				_ = Current()
			}
		}(i)
	}
	wg.Wait()
	if err := Set(Mainnet); err != nil {
		t.Fatal(err)
	}
}
