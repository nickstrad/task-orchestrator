package node

import "testing"

func TestStatsURL(t *testing.T) {
	tests := []struct {
		name    string
		nodeAPI string
		want    string
	}{
		{
			name:    "appends the stats path to the node api",
			nodeAPI: "http://localhost:3001",
			want:    "http://localhost:3001/stats",
		},
		{
			name:    "an api with a trailing slash doubles it",
			nodeAPI: "http://localhost:3001/",
			want:    "http://localhost:3001//stats",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := Node{API: tt.nodeAPI}
			if got := n.statsURL(); got != tt.want {
				t.Errorf("statsURL(%q) = %q, want %q", tt.nodeAPI, got, tt.want)
			}
		})
	}
}
