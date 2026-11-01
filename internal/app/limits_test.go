package app

import "testing"

func TestCPULimitRoundsUpAndNeverReachesZero(t *testing.T) {
	cases := []struct{ cpus, pct, want int }{
		{8, 100, 8}, {8, 50, 4}, {8, 80, 7}, {8, 1, 1}, {1, 1, 1}, {3, 34, 2},
	}
	for _, c := range cases {
		if got := cpuLimit(c.cpus, c.pct); got != c.want {
			t.Errorf("cpuLimit(%d, %d) = %d, want %d", c.cpus, c.pct, got, c.want)
		}
	}
}
