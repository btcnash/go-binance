package api

import "testing"

func TestStatsSnapshot(t *testing.T) {
	session := &Session{}
	session.stats.requestsStarted.Add(4)
	session.stats.requestsCompleted.Add(3)
	session.stats.unsolicitedDropped.Add(2)
	stats := session.Stats()
	if stats.RequestsStarted != 4 || stats.RequestsCompleted != 3 || stats.UnsolicitedDropped != 2 {
		t.Fatalf("stats = %+v", stats)
	}
}
