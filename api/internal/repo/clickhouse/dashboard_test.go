package clickhouse

import "testing"

func TestComputeDeltaFromValues(t *testing.T) {
	tests := []struct {
		name   string
		first  float64
		second float64
		want   float64
	}{
		{name: "increase", first: 10, second: 15, want: 50},
		{name: "decrease", first: 20, second: 10, want: -50},
		{name: "zero baseline", first: 0, second: 8, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := computeDeltaFromValues(tt.first, tt.second); got != tt.want {
				t.Fatalf("computeDeltaFromValues(%v, %v) = %v, want %v", tt.first, tt.second, got, tt.want)
			}
		})
	}
}
