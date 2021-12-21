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

func sendRequest(
	ctx context.Context,
	keysPerTxn int,
	batchToCicada int64,
	client smdbrpc.HotshardGatewayClient,
	chooseKey func() uint64,
	isRead bool,
	logical int32) ([]bool, time.Duration) {

	f := false
	walltime := time.Now().UnixNano()
	timestamp := smdbrpc.HLCTimestamp{
		Walltime:    &walltime,
		Logicaltime: &logical,
	}
	var table, index int64 = 53, 1
	ReadCmd, WriteCmd := smdbrpc.Cmd_GET, smdbrpc.Cmd_PUT
	req := smdbrpc.BatchSendTxnsReq{
		Txns: make([]*smdbrpc.TxnReq, batchToCicada),
	}
	for i := 0; i < int(batchToCicada); i++ {

		// make a txn
		req.Txns[i] = &smdbrpc.TxnReq{
			Ops:                make([]*smdbrpc.Op, keysPerTxn),
			Timestamp:          &timestamp,
			IsPromotion:        &f,
			IsTest:             &f,
			IsDemotedTestField: &f,
			TxnId:              nil,
		}

		isDuplicate := make(map[uint64]bool)
		for j := 0; j < keysPerTxn; j++ {

			key := chooseKey()

			// ensure no duplicate keys
			for isDuplicate[key] {
				key = chooseKey()
			}
			isDuplicate[key] = true

			req.Txns[i].Ops[j] = &smdbrpc.Op{
				Cmd:     nil,
				Table:   &table,
				Index:   &index,
				KeyCols: []int64{int64(key)},
				Key:     encodeToCRDB(0),
				Value:   nil,
			}

			if isRead {
				req.Txns[i].Ops[j].Cmd = &ReadCmd
			} else {
				req.Txns[i].Ops[j].Cmd = &WriteCmd
				req.Txns[i].Ops[j].Value = []byte("jennifer")
			}
		}

		// sort keys in order
		if keysPerTxn > 1 {
			sort.Slice(req.Txns[i].Ops, func(x, y int) bool {
				return req.GetTxns()[i].GetOps()[x].GetKeyCols()[0] < req.
					GetTxns()[i].GetOps()[y].GetKeyCols()[0]
			})
		}
	}

	start := time.Now()
	resp, sendErr := client.BatchSendTxns(ctx, &req)
	elapsed := time.Since(start)
	if sendErr != nil {
		log.Printf("Oops, request failed, err %+v\n", sendErr)
		failureResult := make([]bool, batchToCicada)
		for i := 0; i < int(batchToCicada); i++ {
			failureResult[i] = false
		}
		return failureResult, elapsed
	} else {
		successResult := make([]bool, batchToCicada)
		for i := 0; i < int(batchToCicada); i++ {
			successResult[i] = resp.GetTxnResps()[i].GetIsCommitted()
		}
		return successResult, elapsed
	}
}

func sendPromotion(ctx context.Context, batch int,
	client smdbrpc.HotshardGatewayClient, chooseKey func() uint64,
	logical int32) (bool, time.Duration) {

	var walltime = time.Now().UnixNano()

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
		keyBytes := encodeToCRDB(int(key))
		valBytes := []byte("jennifer")
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
	batchToCicada int64,
	keysPerTxn int,
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
				keysPerTxn,
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
			txnsSucceeded, elapsed := sendRequest(ctx,
				keysPerTxn,
				batchToCicada,
				client,
				chooseKey,
				true,
				int32(workerNum))
			if warmupOver {
				for _, didTxnSucceed := range txnsSucceeded {
					if didTxnSucceed {
						readTicks = append(readTicks, elapsed)
					} else {
						log.Println("read request failed")
					}
				}
			}
		} else {
			txnsSucceeded, elapsed := sendRequest(
				ctx,
				keysPerTxn,
				batchToCicada,
				client,
				chooseKey,
				false,
				int32(workerNum))
			if warmupOver {
				for _, didTxnSucceed := range txnsSucceeded {
					if didTxnSucceed {
						writeTicks = append(writeTicks, elapsed)
					} else {
						log.Println("write request failed")
					}
				}
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
				rtp, rp50, rp99 := extractStats(
					[][]time.Duration{readTicks},
					time.Second)
				wtp, wp50, wp99 := extractStats(
					[][]time.Duration{writeTicks},
					time.Second)
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
	p50Us, _ := stats.Median(allTicks)
	p99Us, _ := stats.Percentile(allTicks, 99)
	p50 = time.Duration(p50Us) * time.Microsecond
	p99 = time.Duration(p99Us) * time.Microsecond

	return throughput, p50, p99
}

func promotekeyspace(address string, keyspace uint64, stepsize uint64) {
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
		promotionReq := smdbrpc.PromoteKeysToCicadaReq{
			Keys: make([]*smdbrpc.Key, stepsize),
		}
		for i := 0; uint64(i) < stepsize; i++ {
			k := int64(key) + int64(i)
			promotionReq.Keys[i] = &smdbrpc.Key{
				Table:     &table,
				Index:     &index,
				KeyCols:   []int64{k},
				Key:       encodeToCRDB(int(k)),
				Timestamp: &smdbrpc.HLCTimestamp{
					Walltime:    &wall,
					Logicaltime: &logical,
				},
				Value:     []byte("jennifer"),
			}
		}
		_, _ = client.PromoteKeysToCicada(ctx, &promotionReq)
		cancel()
	}
}

func main() {
	log.Println("started!")

	batchToCicada := flag.Int64("batchToCicada", 25,
		"number of txns per batch to Cicada")
	keysPerTxn := flag.Int("keysPerTxn", 1, "number of keys per txn")
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
	stepsize := flag.Uint64("stepsize", 5000,
		"stepsize to use when promoting keys in warmup, NOT testPromotion")
	enablePromotion := flag.Bool("enablePromotion", false, "enable warmup promotion")
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
		if i == 0 && *enablePromotion{
			promotekeyspace(address, *keyspace, *stepsize)
			log.Printf("sleeping before testing...")
			time.Sleep(5 * time.Second)
		}
		go worker(address,
			*batchToCicada,
			*keysPerTxn,
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

	tpR, p50R, p99R := extractStats(ticksAcrossWorkersRead, *duration)
	tpW, p50W, p99W := extractStats(ticksAcrossWorkersWrite, *duration)

	log.Printf("Read throughput / p50 / p99 %+v qps / %+v / %+v\n",
		tpR, p50R, p99R)
	log.Printf("Write throughput / p50 / p99 %+v qps / %+v / %+v\n",
		tpW, p50W, p99W)
}
