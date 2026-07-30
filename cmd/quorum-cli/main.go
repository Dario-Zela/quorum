// quorum-cli talks to a quorum cluster.
//
//	quorum-cli -cluster "1=localhost:7201,2=localhost:7202" put k v
//	quorum-cli -cluster ... get k          (linearizable)
//	quorum-cli -cluster ... get -stale k
//	quorum-cli -cluster ... cas k old new
//	quorum-cli -cluster ... del k
//	quorum-cli -cluster ... status
//	quorum-cli -cluster ... watch          (1s-refresh cluster table)
//	quorum-cli -cluster ... bench -clients 32 -duration 30s
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Dario-Zela/quorum/client"
)

func parseCluster(s string) map[uint64]string {
	out := map[uint64]string{}
	for _, kvp := range strings.Split(s, ",") {
		parts := strings.SplitN(strings.TrimSpace(kvp), "=", 2)
		if len(parts) != 2 {
			continue
		}
		id, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil {
			continue
		}
		out[id] = parts[1]
	}
	return out
}

func main() {
	cluster := flag.String("cluster", "", "comma-separated id=host:port client addresses")
	flag.Parse()
	addrs := parseCluster(*cluster)
	if len(addrs) == 0 {
		fmt.Fprintln(os.Stderr, "need -cluster \"1=host:port,...\"")
		os.Exit(2)
	}
	args := flag.Args()
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "need a subcommand: put get del cas status watch bench")
		os.Exit(2)
	}
	c := client.New(addrs)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	switch args[0] {
	case "put":
		must(len(args) == 3, "put <key> <value>")
		fatal(c.Put(ctx, args[1], args[2]))
		fmt.Println("OK")
	case "del":
		must(len(args) == 2, "del <key>")
		fatal(c.Delete(ctx, args[1]))
		fmt.Println("OK")
	case "cas":
		must(len(args) == 4, "cas <key> <old> <new>")
		ok, err := c.CAS(ctx, args[1], args[2], args[3])
		fatal(err)
		fmt.Println("swapped:", ok)
	case "get":
		fs := flag.NewFlagSet("get", flag.ExitOnError)
		stale := fs.Bool("stale", false, "stale read (any node, no protocol)")
		fs.Parse(args[1:])
		must(fs.NArg() == 1, "get [-stale] <key>")
		var v string
		var err error
		if *stale {
			v, err = c.GetStale(ctx, fs.Arg(0))
		} else {
			v, err = c.Get(ctx, fs.Arg(0))
		}
		fatal(err)
		fmt.Printf("%q\n", v)
	case "status":
		printStatus(ctx, c)
	case "watch":
		for {
			fmt.Print("\033[H\033[2J") // clear
			fmt.Println(time.Now().Format("15:04:05"), "— quorum cluster")
			printStatus(ctx, c)
			time.Sleep(1 * time.Second)
		}
	case "bench":
		fs := flag.NewFlagSet("bench", flag.ExitOnError)
		clients := fs.Int("clients", 32, "concurrent client sessions")
		duration := fs.Duration("duration", 30*time.Second, "run length")
		readFrac := fs.Float64("reads", 0.5, "fraction of ops that are reads")
		stale := fs.Bool("stale", false, "use stale reads instead of ReadIndex")
		fs.Parse(args[1:])
		bench(addrs, *clients, *duration, *readFrac, *stale)
	default:
		fmt.Fprintln(os.Stderr, "unknown subcommand", args[0])
		os.Exit(2)
	}
}

func printStatus(ctx context.Context, c *client.Client) {
	fmt.Printf("%-5s %-10s %-6s %-8s %-9s %-6s\n", "node", "role", "term", "commit", "applied", "hint")
	for _, id := range c.IDs() {
		stCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		st, err := c.Status(stCtx, id)
		cancel()
		if err != nil {
			fmt.Printf("%-5d %-10s\n", id, "DOWN")
			continue
		}
		fmt.Printf("%-5d %-10s %-6d %-8d %-9d %-6d\n", id, st.Role, st.Term, st.CommitIndex, st.LastApplied, st.LeaderHint)
	}
}

// bench runs a closed-loop workload and reports throughput + latency.
func bench(addrs map[uint64]string, clients int, duration time.Duration, readFrac float64, stale bool) {
	var ops, errs atomic.Int64
	var mu sync.Mutex
	var lats []time.Duration
	errKinds := map[string]int{}

	deadline := time.Now().Add(duration)
	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			c := client.New(addrs)
			ctx := context.Background()
			key := fmt.Sprintf("bench-%d", worker%16)
			n := 0
			for time.Now().Before(deadline) {
				opCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
				start := time.Now()
				var err error
				if float64(n%100)/100 < readFrac {
					if stale {
						_, err = c.GetStale(opCtx, key)
					} else {
						_, err = c.Get(opCtx, key)
					}
				} else {
					err = c.Put(opCtx, key, fmt.Sprintf("v%d-%d", worker, n))
				}
				cancel()
				n++
				if err != nil {
					errs.Add(1)
					mu.Lock()
					errKinds[err.Error()]++
					mu.Unlock()
					continue
				}
				ops.Add(1)
				mu.Lock()
				lats = append(lats, time.Since(start))
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	pct := func(p float64) time.Duration {
		if len(lats) == 0 {
			return 0
		}
		return lats[int(float64(len(lats)-1)*p)]
	}
	fmt.Printf("clients=%d duration=%s reads=%.0f%% stale=%v\n", clients, duration, readFrac*100, stale)
	fmt.Printf("ops=%d errs=%d throughput=%.0f ops/s\n", ops.Load(), errs.Load(), float64(ops.Load())/duration.Seconds())
	fmt.Printf("latency p50=%s p99=%s\n", pct(0.50), pct(0.99))
	for msg, n := range errKinds {
		fmt.Printf("error[%d]: %s\n", n, msg)
	}
}

func must(ok bool, usage string) {
	if !ok {
		fmt.Fprintln(os.Stderr, "usage:", usage)
		os.Exit(2)
	}
}

func fatal(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
