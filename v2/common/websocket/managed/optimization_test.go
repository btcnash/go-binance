package managed

import "testing"

func TestCurrentGenerationPublicationTracksCurrentSession(t *testing.T) {
	connection := &Connection{}
	session := &physicalSession{generation: 9}
	connection.setCurrent(session)
	if got := connection.currentGeneration.Load(); got != 9 {
		t.Fatalf("published generation = %d, want 9", got)
	}
	if !connection.isCurrentGeneration(9) {
		t.Fatal("current generation was not recognized")
	}
	connection.clearCurrent(session)
	if got := connection.currentGeneration.Load(); got != 0 {
		t.Fatalf("published generation after clear = %d, want 0", got)
	}
	if connection.isCurrentGeneration(9) {
		t.Fatal("cleared generation remained current")
	}
}
