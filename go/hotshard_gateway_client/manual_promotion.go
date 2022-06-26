package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
    //"fmt"
	"flag"
	"google.golang.org/grpc"
	"log"
	"math"
	smdbrpc "smdbrpc/go/build/gen"
	"sort"
	"strings"
	"sync"
	"time"
)

func reverse(numbers []int64) []int64 {
	for i := 0; i < len(numbers)/2; i++ {
		j := len(numbers) - i - 1
		numbers[i], numbers[j] = numbers[j], numbers[i]
	}
	return numbers
}

func dec2baseN(decimal int64, baseN int) (digits []int64) {
	for decimal > 0 {
		digit := decimal % int64(baseN)
		digits = append(digits, digit)
		decimal /= int64(baseN)
	}
	digits = reverse(digits)
	return digits
}

func convertToBase256(decimal int64) (digits []int64) {
	return dec2baseN(decimal, 256)
}

func encodeToCRDB(key int64) (encoding []byte) {
	encoding = append(encoding, byte(189), byte(137))
	if key < 110 {
		encoding = append(encoding, byte(136+key))
	} else {
		digits := convertToBase256(key)
		encoding = append(encoding, byte(245+len(digits)))
		for _, digit := range digits {
			encoding = append(encoding, byte(digit))
		}
	}
	encoding = append(encoding, byte(136))
	return encoding
}

func randomizeHash(key int64, keyspace int64) int64 {
	byteKey := make([]byte, 8)
	binary.BigEndian.PutUint64(byteKey, uint64(key))
	hashed32Bytes := sha256.Sum256(byteKey)
	hashed := make([]byte, 32)
	for i, b := range hashed32Bytes {
		hashed[i] = b
	}
	hashedUint64 := binary.BigEndian.Uint64(hashed)
	hashedModulo := hashedUint64 % uint64(keyspace+1)
	return int64(hashedModulo)
}

func jenkyFixedBytes(key int64, keyspace int64) int64 {
	var constant int64 = 256
	for keyspace > constant {
		constant *= 256
	}

	constant = int64(math.Pow(256, 5))

	return key + constant
}

func transformKey(basekey int64, keyspace int64,
	hash_randomize_keyspace bool, enable_fixed_sized_encoding bool) (
	key int64) {
    key = basekey
    if hash_randomize_keyspace {
		key = randomizeHash(basekey, keyspace)
	}

	if enable_fixed_sized_encoding {
		key = jenkyFixedBytes(key, keyspace)
	}

	return key
}

func promoteKeysToCicada(keys []int64, walltime int64, logical int32,
	client smdbrpc.HotshardGatewayClient, totalKeyspace int64,
	hashRandomizeKeyspace bool, enableFixedSizedEncoding bool) {

	request := smdbrpc.PromoteKeysToCicadaReq{
		Keys: make([]*smdbrpc.Key, len(keys)),
	}
	//val_len_marker := []int64{4, 4, 4, 4}
	//key_len_marker := []int64{4, 4, 4, 4}
	//// key goes here
	//zero_len := []int64{1}
	//wall_time_len := []int64{8, 8, 8, 8, 8, 8, 8, 8}
	//logical_len := []int64{4, 4, 4, 4}
	//timestamp_bookend_len := []int64{1}

	checksum := []int64{82, 196, 81, 94}
	who_knows := []int64{10, 38, 8}
	jennifer := []int64{106, 101, 110, 110, 105, 102, 101, 114}
	jennifers := []int64{}
	for i := 0; i < 64; i ++ {
		jennifers = append(jennifers, jennifer...)
	}
	var val []int64
	val = append(val, checksum...)
	val = append(val, who_knows...)
	val = append(val, jennifers...)
	valBytes := make([]byte, len(val))

	for i, b := range val {
		valBytes[i] = byte(b)
	}

	for i, cicadaKey := range keys {
		crdbKey := transformKey(cicadaKey, totalKeyspace, hashRandomizeKeyspace,
			enableFixedSizedEncoding)
		var table, index int64 = 53, 1
		cicadaKeyCols := []int64{cicadaKey}
		keyBytes := encodeToCRDB(crdbKey)
		request.Keys[i] = &smdbrpc.Key{
			Table:         &table,
			Index:         &index,
			CicadaKeyCols: cicadaKeyCols,
			Key:           keyBytes,
			Timestamp: &smdbrpc.HLCTimestamp{
				Walltime:    &walltime,
				Logicaltime: &logical,
			},
			Value: valBytes,
			CrdbKeyCols: []int64{crdbKey},
		}
	}

	sort.Slice(request.Keys, func(i, j int) bool {
		return request.Keys[i].CicadaKeyCols[0] < request.Keys[j].
			CicadaKeyCols[0]
	})

	// promote to cicada
	reply, err := client.PromoteKeysToCicada(context.Background(), &request)
	if err != nil {
		log.Fatalf("Failed to send, err %+v\n", err)
	} else {
		for _, didKeySucceed := range reply.GetSuccessfullyPromoted() {
			if !didKeySucceed {
				log.Fatalf("Key did not get promoted\n")
			}
		}
	}
    //log.Printf("up to %lu\n", keys[0])
}

func updateCRDBPromotionMaps(keys []int64, walltime int64, logical int32,
	clients []smdbrpc.HotshardGatewayClient, totalKeyspace int64,
	hashRandomizeKeyspace bool, enableFixedSizedEncoding bool) {

	// populate promotion request
	updateMapReq := smdbrpc.PromoteKeysReq{
		Keys: make([]*smdbrpc.KVVersion, len(keys)),
	}
	for i, cicadaKey := range keys {
		crdbKey := transformKey(cicadaKey, totalKeyspace, hashRandomizeKeyspace,
			enableFixedSizedEncoding)
		updateMapReq.Keys[i] = &smdbrpc.KVVersion{
			Key:   encodeToCRDB(crdbKey),
			Value: nil,
			Timestamp: &smdbrpc.HLCTimestamp{
				Walltime:    &walltime,
				Logicaltime: &logical,
			},
			Hotness: nil,
			CicadaKeyCols: []int64{cicadaKey},
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < len(clients); i++ {
		wg.Add(1)
		go func(clientIdx int) {
			defer wg.Done()
			client := clients[clientIdx]
			crdbCtx, crdbCancel := context.WithTimeout(context.Background(),
				time.Minute)
			defer crdbCancel()

			resp, err := client.UpdatePromotionMap(crdbCtx, &updateMapReq)
			if err != nil {
				log.Fatalf("cannot send updatePromoMapReq CRDB node %d"+
					"err %+v\n", clientIdx, err)
			}

			for _, keyMigrationResp := range resp.WereSuccessfullyMigrated {
				if !keyMigrationResp.GetIsSuccessfullyMigrated() {
					log.Fatalf("did not update all keys in map CRDB node %d",
						clientIdx)
				}
			}

		}(i)
	}
	wg.Wait()
}

type Wrapper struct {
	Addr    string
	ConnPtr *grpc.ClientConn
	Client  smdbrpc.HotshardGatewayClient
}

func grpcConnect(wrapper *Wrapper) {
	var err error
	wrapper.ConnPtr, err = grpc.Dial(wrapper.Addr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("failed to dial %+v\n", wrapper.Addr)
	}
	wrapper.Client = smdbrpc.NewHotshardGatewayClient(wrapper.ConnPtr)
}

func promoteKeys(keys []int64, batch int, walltime int64, logical int32,
	cicadaAddr string, crdbAddresses []string, totalKeyspace int64,
	hashRandomizeKeyspace bool, enableFixedSizedEncoding bool) {

	// connect to Cicada
	numClients := 16
    //numClients := 1
	cicadaWrappers := make([]Wrapper, numClients)
	for i := 0; i < numClients; i++ {
		cicadaWrappers[i] = Wrapper{
			Addr: cicadaAddr,
		}
		grpcConnect(&cicadaWrappers[i])
	}

	// connect to CRDB
	crdbWrappers := make([]Wrapper, len(crdbAddresses))
	for i, crdbAddr := range crdbAddresses {
		crdbWrappers[i] = Wrapper{
			Addr: crdbAddr,
		}
		grpcConnect(&crdbWrappers[i])
	}
	crdbClients := make([]smdbrpc.HotshardGatewayClient, len(crdbWrappers))
	for i, wrapper := range crdbWrappers {
		crdbClients[i] = wrapper.Client
	}

	// promote keys in batches
	inflightBatches := 0
	var wg sync.WaitGroup
	for batchFloor := 0; batchFloor < len(keys); batchFloor += batch {
		batchCeiling := math.Min(float64(batchFloor+batch), float64(len(keys)))
		wg.Add(1)
		go func(i int, max int, clientIdx int) {
			defer wg.Done()
			promoteKeysToCicada(keys[i:max], walltime, logical,
				cicadaWrappers[clientIdx].Client, totalKeyspace,
				hashRandomizeKeyspace, enableFixedSizedEncoding)
			updateCRDBPromotionMaps(keys[i:max], walltime, logical,
				crdbClients, totalKeyspace, hashRandomizeKeyspace,
				enableFixedSizedEncoding)
		}(batchFloor, int(batchCeiling), inflightBatches)
		inflightBatches++
		if inflightBatches%numClients == 0 {
			wg.Wait()
			inflightBatches = 0
		}
	}
	if inflightBatches > 0 {
		wg.Wait()
	}
}

func main() {
	batch := flag.Int("batch", 1,
		"number of keys to promote in a single batch")
	cicadaAddr := flag.String("cicadaAddr", "node-11:50051",
		"cicada host machine")
	crdbAddrs := flag.String("crdbAddrs", "node-8:50055,node-9:50055",
		"csv of crdb addresses")
	keyMin := flag.Int64("keyMin", 0, "minimum key to promote")
	keyMax := flag.Int64("keyMax", 0, "one over the maximum key to promote")
	keyspace := flag.Int64("keyspace", 400000000, "total keyspace")
	hash_randomize_keyspace := flag.Bool("hash_randomize_keyspace", true,
		"whether to hash the keyspace so hotkeys aren't contiguous")
	enable_fixed_sized_encoding := flag.Bool(
		"enable_fixed_sized_encoding", true,
		"whether to disable adding a constant to keyspace to keep all keys" +
			" the same size")
	flag.Parse()

	crdbAddrsSlice := strings.Split(*crdbAddrs, ",")

	log.Printf("batch %d, cicadaAddr %s, crdbAddrs %+s\n", *batch, *cicadaAddr,
		crdbAddrsSlice)

	walltime := time.Now().UnixNano()
	var logical int32 = 0

    tic := time.Now()
	if *keyMax-*keyMin > 0 {
		keys := make([]int64, *keyMax-*keyMin)
		for i := int64(0); i < *keyMax-*keyMin; i++ {
			//keys[i] = transformKey(i + *keyMin, *keyspace,
			//	*hash_randomize_keyspace, *enable_fixed_sized_encoding)
			keys[i] = i
		}
		sort.Slice(keys, func(i, j int) bool {
			return keys[i] < keys[j]
		})
		promoteKeys(keys, *batch, walltime, logical, *cicadaAddr,
			crdbAddrsSlice, *keyspace, *hash_randomize_keyspace,
			*enable_fixed_sized_encoding)
	}
    toc := time.Since(tic)
    log.Printf("elapsed %+v\n", toc)
}