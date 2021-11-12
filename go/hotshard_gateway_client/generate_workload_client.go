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
	isRead bool, logicalTime int32) (bool, time.Duration) {

	var walltime = time.Now().UnixNano()
	var logical int32 = logicalTime
	f := false

	request := smdbrpc.TxnReq{
		Ops: make([]*smdbrpc.Op, batch),
		Timestamp: &smdbrpc.HLCTimestamp{
			Walltime:    &walltime,
			Logicaltime: &logical,
		},
		IsPromotion:        &f,
		IsTest:             &f,
		IsDemotedTestField: &f,
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
		var table, index int64 = 53, 1
		keyCols := []int64{int64(key)}
		keyBytes := []byte("key")
		if isRead {
			cmd := smdbrpc.Cmd_GET
			request.Ops[i] = &smdbrpc.Op{
				Cmd:     &cmd,
				Table:   &table,
				Index:   &index,
				KeyCols: keyCols,
				Key:     keyBytes,
			}
		} else {
			cmd := smdbrpc.Cmd_PUT
			valBytes := []byte("val")
			request.Ops[i] = &smdbrpc.Op{
				Cmd:     &cmd,
				Table:   &table,
				Index:   &index,
				KeyCols: keyCols,
				Key:     keyBytes,
				Value:   valBytes,
			}
		}
	}

	sort.Slice(request.Ops, func(i, j int) bool {
		return request.Ops[i].KeyCols[0] < request.Ops[i].KeyCols[0]
	})

	start := time.Now()
	reply, err := client.SendTxn(ctx, &request)
	elapsed := time.Since(start)
	if err != nil {
		log.Printf("Oops, request failed, err %+v\n", err)
		return false, -1
	} else {
		return reply.GetIsCommitted(), elapsed
	}
}

func sendPromotion(ctx context.Context, batch int,
	client smdbrpc.HotshardGatewayClient, chooseKey func() uint64,
	logicalTime int32) (bool, time.Duration) {

	var walltime = time.Now().UnixNano()
	var logical int32 = logicalTime

	request := smdbrpc.PromoteKeysToCicadaReq{
		Keys: make([]*smdbrpc.Key, batch),
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
		var table, index int64 = 53, 1
		keyCols := []int64{int64(key)}
		keyBytes := []byte("key")
		valBytes := []byte("val")
		request.Keys[i] = &smdbrpc.Key{
			Table:   &table,
			Index:   &index,
			KeyCols: keyCols,
			Key:     keyBytes,
			Timestamp: &smdbrpc.HLCTimestamp{
				Walltime:    &walltime,
				Logicaltime: &logical,
			},
			Value:   valBytes,
		}
	}

	sort.Slice(request.Keys, func(i, j int) bool {
		return request.Keys[i].KeyCols[0] < request.Keys[i].KeyCols[0]
	})

	start := time.Now()
	reply, err := client.PromoteKeysToCicada(ctx, &request)
	elapsed := time.Since(start)
	if err != nil {
		log.Printf("Oops, request failed, err %+v\n", err)
		return false, -1
	} else {
		succeeded := true
		for _, didKeySucceed := range reply.GetSuccessfullyPromoted()  {
			if !didKeySucceed {
				succeeded = false
				break
			}
		}
		return succeeded, elapsed
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
	warmup time.Duration,
	workerNum int,
	testPromotion bool) {

	// decrement wait group at the end
	defer wg.Done()

	// connecting to server
	conn, err := grpc.Dial(address,
		grpc.WithInsecure(),
		//grpc.WithBlock(),
	)
	if err != nil {
		log.Fatalf("did not connect: %+v\n", err)
	}
	defer func(conn *grpc.ClientConn) {
		_ = conn.Close()
	}(conn)
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
		if testPromotion {
			if ok, elapsed := sendPromotion(ctx,
				batch,
				client,
				chooseKey,
				int32(workerNum)); ok {
				if warmupOver {
					readTicks = append(readTicks, elapsed)
				}
			} else {
				log.Println("promotion request failed")
			}
		} else if r := rand.Intn(100); r < readPercent {
			if ok, elapsed := sendRequest(ctx,
				batch,
				client,
				chooseKey,
				true,
				int32(workerNum)); ok {
				if warmupOver {
					readTicks = append(readTicks, elapsed)
				}
			} else {
				log.Println("read request failed")
			}
		} else {
			if ok, elapsed := sendRequest(ctx, batch, client,
				chooseKey, false, int32(workerNum)); ok {
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
				rtp, rp50, rp99 := extractStats([][]time.Duration{readTicks}, time.Second, batch)
				wtp, wp50, wp99 := extractStats([][]time.Duration{writeTicks}, time.Second, batch)
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
	duration time.Duration, batch int) (throughput float64, p50, p99 time.Duration) {

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

	return throughput * float64(batch), p50, p99
}

func promotekeyspace(address string, chooseKey func() uint64, keyspace uint64, stepsize uint64) {
	conn, err := grpc.Dial(address,
		grpc.WithInsecure(),
		//grpc.WithBlock(),
	)
	if err != nil {
		log.Fatalf("did not connect: %+v\n", err)
	}
	defer func(conn *grpc.ClientConn) {
		_ = conn.Close()
	}(conn)
	client := smdbrpc.NewHotshardGatewayClient(conn)

	var table, index int64 = 53, 1
	wall, logical := time.Now().UnixNano(), int32(0)
	for key := uint64(0); key < keyspace; key += stepsize {
		ctx, cancel := context.WithTimeout(context.Background(), 5 * time.Second)
		defer cancel()
		promotionReq := smdbrpc.PromoteKeysToCicadaReq{
			Keys: make([]*smdbrpc.Key, stepsize),
		}
		for i := 0; uint64(i) < stepsize; i++ {
			k := int64(key) + int64(i)
			promotionReq.Keys[i] = &smdbrpc.Key{
				Table:     &table,
				Index:     &index,
				KeyCols:   []int64{int64(k)},
				Key:       []byte("key"),
				Timestamp: &smdbrpc.HLCTimestamp{
					Walltime:    &wall,
					Logicaltime: &logical,
				},
				Value:     []byte("value"),
			}
		}
		_, _ = client.PromoteKeysToCicada(ctx, &promotionReq)
	}
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
	instantaneousStats := flag.Bool("instantaneousStats", true, "show per second stats")
	warmup := flag.Duration("warmup", 1*time.Second, "warmup duration")
	testPromotion := flag.Bool("testPromotion", false, "to test using promotion requests or not")
	stepsize := flag.Uint64("stepsize", 100, "stepsize to use when promoting keys in warmup, NOT testPromotion")
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
		if i == 0 {
			promotekeyspace(address, func() uint64 { return rng.Uint64() % *keyspace }, *keyspace, *stepsize)
		}
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
			*warmup,
			i,
			*testPromotion)
	}
	wg.Wait()

	tp_r, p50_r, p99_r := extractStats(ticksAcrossWorkersRead, *duration, *batch)
	tp_w, p50_w, p99_w := extractStats(ticksAcrossWorkersWrite, *duration, *batch)

	log.Printf("Read throughput / p50 / p99 %+v qps / %+v / %+v\n",
		tp_r, p50_r, p99_r)
	log.Printf("Write throughput / p50 / p99 %+v qps / %+v / %+v\n",
		tp_w, p50_w, p99_w)
}
