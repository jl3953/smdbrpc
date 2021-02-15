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

const (
	WRITE_COUNTER = iota
	READ_COUNTER
	NUM_COUNTERS
)

func writeRequest(ctx context.Context, batch int,
	client smdbrpc.HotshardGatewayClient, chooseKey func() uint64,
	counter *sync.Map) bool {

	var walltime = time.Now().UnixNano()
	var logical int32 = 214

	request := smdbrpc.HotshardRequest{
		Hlctimestamp: &smdbrpc.HLCTimestamp{
			Walltime:    &walltime,
			Logicaltime: &logical,
		},
		WriteKeyset: make([]*smdbrpc.KVPair, batch),
		ReadKeyset:  nil,
	}

	set := make(map[uint64]bool, 0)
	for i := 0; i < batch; i++ {
		key := chooseKey()
		for set[key] {
			key = chooseKey()
		}
		set[key] = true
		val := key
		request.WriteKeyset[i] = &smdbrpc.KVPair{
			Key:   &key,
			Value: &val,
		}
	}
	sort.Slice(request.WriteKeyset, func(i, j int) bool {
		return request.WriteKeyset[i].GetKey() < request.WriteKeyset[j].GetKey()
	})
	start := time.Now()
	reply, err := client.ContactHotshard(ctx, &request)
	elapsed := time.Since(start)
	if nil != err {
		log.Printf("Oops, write request failed, err %+v\n", err)
		return false
	} else {
		counter.Store(elapsed, true)
		return reply.GetIsCommitted()
	}
}

func readRequest(ctx context.Context, batch int,
	client smdbrpc.HotshardGatewayClient, chooseKey func() uint64,
	counter *sync.Map) bool {

	// set the request timestamp
	var walltime = time.Now().UnixNano()
	var logical int32 = 214

	// fill out request template
	request := smdbrpc.HotshardRequest{
		Hlctimestamp: &smdbrpc.HLCTimestamp{
			Walltime:    &walltime,
			Logicaltime: &logical,
		},
		ReadKeyset: make([]uint64, batch),
	}

	// populate request keys
	set := make(map[uint64]bool, 0)
	for i := 0; i < batch; i++ {
		key := chooseKey()
		for set[key] {
			key = chooseKey()
		}
		set[key] = true
		request.ReadKeyset[i] = key
	}
	sort.Slice(request.ReadKeyset, func(i, j int) bool {
		return request.ReadKeyset[i] < request.ReadKeyset[j]
	})

	// send request and time it
	start := time.Now()
	reply, err := client.ContactHotshard(ctx, &request)
	elapsed := time.Since(start)
	if nil != err {
		log.Printf("Oops, read request failed, err %+v\n", err)
		return false
	} else {
		counter.Store(elapsed, true)
		return reply.GetIsCommitted()
	}
}

func worker(address string, batch int, duration time.Duration, readPercent int,
	chooseKey func() uint64, wg *sync.WaitGroup, histogram []sync.Map) {

	// make sure wait group is decremented
	defer wg.Done()

	// connect to target host as client
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %+v", err)
	}
	defer conn.Close()
	client := smdbrpc.NewHotshardGatewayClient(conn)

	// I have no idea

	// query host only for set duration
	ticker := time.NewTicker(duration)
	//tickerSec := time.NewTicker(time.Second)
	//i := 0
	writeCounter := 0
	readCounter := 0
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		if r := rand.Intn(100); r < readPercent {
			//log.Printf("r %d\n", r)
			readRequest(ctx, batch, client, chooseKey, &histogram[READ_COUNTER])
			readCounter++
		} else {
			//log.Printf("r %d\n", r)
			writeRequest(ctx, batch, client, chooseKey, &histogram[WRITE_COUNTER])
			writeCounter++
		}
		cancel()
		select {
		case <-ticker.C:
			//log.Printf("%+v passed", duration)
			return
		//case <-tickerSec.C:
		//	log.Printf("second %d, read/write throughput %d/%d\n", i, readCounter, writeCounter)
		//	i++
		//	readCounter = 0
		//	writeCounter = 0
		default:

		}
	}
}

func main() {

	fmt.Printf("started!\n")

	batch := flag.Int("batch", 1, "number of keys per batch")
	concurrency := flag.Int("concurrency", 1, "number of concurrent clients")
	duration := flag.Duration("duration", 1*time.Second, "duration for which to run")
	host := flag.String("host", "localhost", "target host")
	keyspace := flag.Uint64("keyspace", 1000, "keyspace from 0 to specified")
	port := flag.Int("port", 50051, "target port")
	readPercent := flag.Int("read_percent", 0, "read percentage, int")
	zipfianSkew := flag.Float64("s", 1.2, "zipfian skew s, must be greater than 1")
	flag.Parse()

	r := rand.New(rand.NewSource(time.Now().Unix()))
	zipf := rand.NewZipf(r, *zipfianSkew, 1, *keyspace)
	if nil == zipf {
		log.Fatalf("nil zipf, is your --s greater than 1?\n")
	}
	address := fmt.Sprintf("%s:%d", *host, *port)
	var wg sync.WaitGroup
	histogram := make([]sync.Map, NUM_COUNTERS)
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go worker(address,
			*batch,
			*duration,
			*readPercent,
			func() uint64 { return zipf.Uint64() },
			&wg,
			histogram)
	}
	wg.Wait()

	readThroughput, p50_read, p99_read := extractStats(&histogram[READ_COUNTER], *duration)
	writeThroughput, p50_write, p99_write := extractStats(&histogram[WRITE_COUNTER], *duration)

	log.Printf("read_throughput %+v, p50 %+v, p99 %+v\n", readThroughput, p50_read, p99_read)
	log.Printf("write_throughput %+v, p50 %+v, p99 %+v\n", writeThroughput, p50_write, p99_write)
}

func extractStats(writeCounter *sync.Map, duration time.Duration) (throughput float64, p50 time.Duration,
	p99 time.Duration) {
	counterSlice := make([]float64, 0)
	writeCounter.Range(func(key interface{}, _ interface{}) bool {
		elapsed := key.(time.Duration)
		counterSlice = append(counterSlice, float64(elapsed.Microseconds()))
		return true
	})

	p50_us, _ := stats.Median(counterSlice)
	p99_us, _ := stats.Percentile(counterSlice, 99)
	totalThroughput := len(counterSlice)
	throughput = float64(totalThroughput) / duration.Seconds()
	p50 = time.Duration(p50_us) * time.Microsecond
	p99 = time.Duration(p99_us) * time.Microsecond

	return throughput, p50, p99
}
