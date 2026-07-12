package private

import (
	"testing"
	"time"
)

func TestPrivateSessionDefaultsProactiveRotation(t *testing.T) {
	opts := managedConnectionOptions(ConnectionOptions{})
	want := 23*time.Hour + 50*time.Minute
	if opts.MaxConnectionAge != want {
		t.Fatalf("max age = %s, want %s", opts.MaxConnectionAge, want)
	}
}

func TestPrivateSessionCanDisableProactiveRotation(t *testing.T) {
	opts := managedConnectionOptions(ConnectionOptions{DisableRotation: true})
	if opts.MaxConnectionAge != 0 {
		t.Fatalf("max age = %s, want disabled", opts.MaxConnectionAge)
	}
}
