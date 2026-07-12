package managed

import "testing"

func BenchmarkIsCurrentGeneration(b *testing.B) {
	connection := &Connection{generation: 42}
	connection.setCurrent(&physicalSession{generation: 42})
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if !connection.isCurrentGeneration(42) {
			b.Fatal("generation changed")
		}
	}
}
