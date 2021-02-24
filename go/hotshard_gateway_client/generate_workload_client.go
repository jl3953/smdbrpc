package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/montanaflynn/stats"
	"google.golang.org/grpc"
	"log"
	"math/rand"
	smdbrpc "smdbrpc/go/build/gen"
	"sort"
	"sync"
	"time"
)

func sendRequest(ctx context.Context, batch int,
	client smdbrpc.HotshardGatewayClient, chooseKey func() uint64,
	isRead bool) (bool, time.Duration) {

	var walltime = time.Now().UnixNano()
	var logical int32 = 214

	request := smdbrpc.HotshardRequest{
		Hlctimestamp: &smdbrpc.HLCTimestamp{
			Walltime:    &walltime,
			Logicaltime: &logical,
		},
	}
	if isRead {
		request.ReadKeyset = make([]uint64, batch)
	} else {
		request.WriteKeyset = make([]*smdbrpc.KVPair, batch)
	}

	isDuplicate := make(map[uint64]bool, 0)
	for i := 0; i < batch; i++ {

		// choose key
		key := chooseKey()
		for isDuplicate[key] {
			key = chooseKey()
		}
		isDuplicate[key] = true

		// populate read or write request list
		if isRead {
			request.ReadKeyset[i] = key
		} else {
			val := key
			request.WriteKeyset[i] = &smdbrpc.KVPair{
				Key:   &key,
				Value: &val,
			}
		}
	}

	if isRead {
		sort.Slice(request.ReadKeyset, func(i, j int) bool {
			return request.ReadKeyset[i] < request.ReadKeyset[j]
		})
	} else {
		sort.Slice(request.WriteKeyset, func(i, j int) bool {
			return request.WriteKeyset[i].GetKey() < request.WriteKeyset[j].GetKey()
		})
	}

	start := time.Now()
	reply, err := client.ContactHotshard(ctx, &request)
	elapsed := time.Since(start)
	if err != nil {
		log.Printf("Oops, request failed, err %+v\n", err)
		return false, -1
	} else {
		return reply.GetIsCommitted(), elapsed
	}
}

func worker(address string,
	batch int,
	duration time.Duration,
	readPercent int,
	chooseKey func() uint64,
	wg *sync.WaitGroup,
	timeout time.Duration,
	durationsRead *[]time.Duration,
	durationsWrite *[]time.Duration,
	instantaneousStats bool,
	warmup time.Duration) {

	// decrement wait group at the end
	defer wg.Done()

	// connecting to server
	conn, err := grpc.Dial(address,
		grpc.WithInsecure(),
		grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %+v\n", err)
	}
	defer conn.Close()
	client := smdbrpc.NewHotshardGatewayClient(conn)

	tickerWarmup := time.NewTicker(warmup)
	ticker := time.NewTicker(warmup + duration)
	tickerInstant := time.NewTicker(time.Second)
	i := 0
	readTicks := make([]time.Duration, 0)
	writeTicks := make([]time.Duration, 0)
	warmupOver := false
	for {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		if r := rand.Intn(100); r < readPercent {
			if ok, elapsed := sendRequest(ctx, batch, client,
				chooseKey, true); ok {
				if warmupOver {
					readTicks = append(readTicks, elapsed)
				}
			} else {
				log.Println("read request failed")
			}
		} else {
			if ok, elapsed := sendRequest(ctx, batch, client,
				chooseKey, false); ok {
				if warmupOver {
					writeTicks = append(writeTicks, elapsed)
				}
			} else {
				log.Println("write request failed")
			}
		}
		cancel()
		select {
		case <-tickerWarmup.C:
			warmupOver = true
			tickerWarmup.Stop()
		case <-ticker.C:
			return
		case <-tickerInstant.C:
			*durationsRead = append(*durationsRead, readTicks...)
			*durationsWrite = append(*durationsWrite, writeTicks...)
			if instantaneousStats && warmupOver {
				rtp, rp50, rp99 := extractStats([][]time.Duration{readTicks}, time.Second)
				wtp, wp50, wp99 := extractStats([][]time.Duration{writeTicks}, time.Second)
				log.Printf("second %+v: r %+v qps / %+v / %+v || w %+v qps / %+v / %+v\n",
					time.Duration(i)*time.Second,
					rtp, rp50, rp99,
					wtp, wp50, wp99)
				i++
			}
			readTicks, writeTicks = []time.Duration{}, []time.Duration{}
		default:

		}
	}
}

func extractStats(ticksAcrossWorkers [][]time.Duration,
	duration time.Duration) (throughput float64, p50, p99 time.Duration) {

	// calculate throughput, p50, p99
	allTicks := make([]float64, 0)
	for _, ticksPerWorker := range ticksAcrossWorkers {
		for _, tick := range ticksPerWorker {
			allTicks = append(allTicks, float64(tick.Microseconds()))
		}
	}
	sort.Slice(allTicks, func(i, j int) bool {
		return allTicks[i] < allTicks[j]
	})
	throughput = float64(len(allTicks)) / duration.Seconds()
	p50_us, _ := stats.Median(allTicks)
	p99_us, _ := stats.Percentile(allTicks, 99)
	p50 = time.Duration(p50_us) * time.Microsecond
	p99 = time.Duration(p99_us) * time.Microsecond

	return throughput, p50, p99
}

func main() {
	log.Println("started!")

	batch := flag.Int("batch", 1, "number of keys per batch")
	concurrency := flag.Int("concurrency", 1, "number of concurrent clients")
	duration := flag.Duration("duration", 1*time.Second, "duration for which to run")
	host := flag.String("host", "localhost", "target host")
	keyspace := flag.Uint64("keyspace", 1000, "keyspace from 0 to specified")
	port := flag.Int("port", 50051, "target port")
	numPorts := flag.Int("numPorts", 1, "number of ports")
	readPercent := flag.Int("read_percent", 0, "read percentage, int")
	timeout := flag.Duration("timeout", 500*time.Millisecond, "timeout for request")
	instantaneousStats := flag.Bool("instantaneousStats", false, "show per second stats")
	warmup := flag.Duration("warmup", 30*time.Second, "warmup duration")
	flag.Parse()

	var wg sync.WaitGroup // wait group

	// keeping elapsed times of all requests across workers
	ticksAcrossWorkersRead := make([][]time.Duration, *concurrency)
	ticksAcrossWorkersWrite := make([][]time.Duration, *concurrency)

	// spin off workers
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)

		address := fmt.Sprintf("%s:%d", *host, *port+rand.Intn(*numPorts)) // server addr
		ticksAcrossWorkersRead[i] = make([]time.Duration, 0)
		ticksAcrossWorkersWrite[i] = make([]time.Duration, 0)
		rng := rand.New(rand.NewSource(int64(i)))
		go worker(address,
			*batch,
			*duration,
			*readPercent,
			func() uint64 { return rng.Uint64() % *keyspace },
			&wg,
			*timeout,
			&ticksAcrossWorkersRead[i],
			&ticksAcrossWorkersWrite[i],
			*instantaneousStats,
			*warmup)
	}
	wg.Wait()

	tp_r, p50_r, p99_r := extractStats(ticksAcrossWorkersRead, *duration)
	tp_w, p50_w, p99_w := extractStats(ticksAcrossWorkersWrite, *duration)

	log.Printf("Read throughput / p50 / p99 %+v qps / %+v / %+v\n",
		tp_r, p50_r, p99_r)
	log.Printf("Write throughput / p50 / p99 %+v qps / %+v / %+v\n",
		tp_w, p50_w, p99_w)
}
