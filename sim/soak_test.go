package sim

import (
	"os"
	"strconv"
	"testing"
)

// TestSoakNightly is the long-haul run: hundreds of seeds of client load
// under the full nemesis with porcupine judging every history. Gated
// behind QUORUM_SOAK so push CI stays fast; the nightly workflow sets it.
//
//	QUORUM_SOAK=1 go test ./sim -run TestSoakNightly -timeout 120m
func TestSoakNightly(t *testing.T) {
	if os.Getenv("QUORUM_SOAK") == "" {
		t.Skip("set QUORUM_SOAK=1 for the nightly soak")
	}
	seeds := int64(2000)
	if s := os.Getenv("QUORUM_SOAK_SEEDS"); s != "" {
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			seeds = n
		}
	}
	for seed := int64(0); seed < seeds; seed++ {
		w := runClients(t, seed, Config{
			Seed: seed, Clients: 5, ClientOps: 40,
			Nemesis: &NemesisConfig{Every: 25, DropRate: 0.05, DupRate: 0.05},
		})
		checkLinearizable(t, seed, w)
		if seed%25 == 0 {
			t.Logf("seed %d/%d clean", seed, seeds)
		}
	}
}
