package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"google.golang.org/grpc"
	"log"
	"math"
	"os"
	smdbrpc "smdbrpc/go/build/gen"
	"strconv"

	_ "github.com/lib/pq"
	"sort"
	"strings"
	"sync"
	"time"
)

func promoteKeysToCicada(keys []Key, walltime int64, logical int32,
	client smdbrpc.HotshardGatewayClient) {

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

	//checksum := []int64{82, 196, 81, 94}
	//who_knows := []int64{10, 38, 8}
	//jennifer := []int64{106, 101, 110, 110, 105, 102, 101, 114}
	//jennifers := []int64{}
	//for i := 0; i < 64; i++ {
	//	jennifers = append(jennifers, jennifer...)
	//}
	//var val []int64
	//val = append(val, checksum...)
	//val = append(val, who_knows...)
	//val = append(val, jennifers...)
	//valBytes := make([]byte, len(val))
	//
	//for i, b := range val {
	//	valBytes[i] = byte(b)
	//}

	for i, key := range keys {

		var table, index int64 = int64(key.TableNum), int64(key.Index)
		request.Keys[i] = &smdbrpc.Key{
			Table:         &table,
			Index:         &index,
			CicadaKeyCols: key.PkCols,
			Key:           key.ByteKey,
			Timestamp: &smdbrpc.HLCTimestamp{
				Walltime:    &walltime,
				Logicaltime: &logical,
			},
			Value:       key.ByteValue,
			CrdbKeyCols: key.PkCols,
			TableName:   &key.TableName,
		}
	}

	sort.Slice(request.Keys, func(i, j int) bool {
		return request.Keys[i].CicadaKeyCols[0] < request.Keys[j].
			CicadaKeyCols[0]
	})

	// promote to cicada
	reply, err := client.PromoteKeysToCicada(context.Background(), &request)
	if err != nil {
		log.Fatalf("Failed to send to Cicada, err %+v\n", err)
	} else {
		for _, didKeySucceed := range reply.GetSuccessfullyPromoted() {
			if !didKeySucceed {
				log.Fatalf("Key did not get promoted\n")
			}
		}
	}
}

func populateCRDBTableName2NumMapping(crdb_node string, client smdbrpc.HotshardGatewayClient, mapping map[string]int32) {

	host := strings.Split(crdb_node, ":")[0]

	req := smdbrpc.PopulateCRDBTableNumMappingReq{
		TableNumMappings: []*smdbrpc.TableNumMapping{},
	}

	for tableName, tableNum := range mapping {
		tempName := tableName
		tempNum := tableNum
		req.TableNumMappings = append(req.TableNumMappings, &smdbrpc.TableNumMapping{
			TableName: &tempName,
			TableNum:  &tempNum,
		})
	}
	_, err := client.PopulateCRDBTableNumMapping(context.Background(), &req)
	if err != nil {
		log.Fatalf("Could not populate on a client, err %+v\n", err)
	} else {
		log.Printf("Populated %s with tableNum\n", host)
	}
}

func updateCRDBPromotionMaps(keys []Key, walltime int64, logical int32,
	clients []smdbrpc.HotshardGatewayClient) {

	// populate promotion request
	updateMapReq := smdbrpc.PromoteKeysReq{
		Keys: make([]*smdbrpc.KVVersion, len(keys)),
	}
	for i, key := range keys {
		updateMapReq.Keys[i] = &smdbrpc.KVVersion{
			Key:   key.ByteKey,
			Value: nil,
			Timestamp: &smdbrpc.HLCTimestamp{
				Walltime:    &walltime,
				Logicaltime: &logical,
			},
			Hotness:       nil,
			CicadaKeyCols: key.PkCols,
			TableName:     &key.TableName,
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
				log.Fatalf("cannot send updatePromoMapReq CRDB node %d, "+
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

func promoteKeys(keys []Key, batch int, walltime int64, logical int32,
	cicadaAddr string, crdbAddresses []string) {

	// connect to Cicada
	//numClients := 16
	numClients := 15
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
		log.Printf("jenndebug crdbAddr [%s]\n", crdbAddr)
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
			promoteKeysToCicada(keys[i:max], walltime, logical, cicadaWrappers[clientIdx].Client)
			updateCRDBPromotionMaps(keys[i:max], walltime, logical, crdbClients)
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

func read_csv_mapping_file(csvmappingfile string) (mapping map[string]int32) {

	// open CSV file
	fd, err := os.Open(csvmappingfile)
	if err != nil {
		fmt.Printf("Opening %s error %+v\n", csvmappingfile, err)
	}
	defer func(fd *os.File) {
		err := fd.Close()
		if err != nil {

		}
	}(fd)

	// read csv file
	mapping = make(map[string]int32)
	fileReader := csv.NewReader(fd)
	for record, _ := fileReader.Read(); record != nil; record, _ = fileReader.Read() {
		tableName := record[0]
		tableNum, _ := strconv.Atoi(record[1])
		mapping[tableName] = int32(tableNum)
	}
	return mapping
}

func deduceWarehouseKeys(tableNum int32, index int, numWarehouses int) (warehouseKeys []Key) {

	for w_id := 0; w_id < numWarehouses; w_id++ {
		key := Key{
			TableName: WAREHOUSE,
			TableNum:  tableNum,
			Index:     index,
			PkCols:    []int64{int64(w_id)},
			ByteKey:   []byte{byte(136 + tableNum), byte(136 + index), byte(136 + w_id), byte(136)},
		}
		warehouseKeys = append(warehouseKeys, key)
	}

	return warehouseKeys
}

func deduceDistrictKeys(tableNum int32, index int, numWarehouses int) (districtKeys []Key) {
	for w_id := 0; w_id < numWarehouses; w_id++ {
		for d_id := 1; d_id <= 10; d_id++ {
			key := Key{
				TableName: DISTRICT,
				TableNum:  tableNum,
				Index:     index,
				PkCols:    []int64{int64(w_id), int64(d_id)},
				ByteKey:   []byte{byte(136 + tableNum), byte(136 + index), byte(136 + w_id), byte(136 + d_id), byte(136)},
			}
			districtKeys = append(districtKeys, key)
		}
	}
	return districtKeys
}

func populateAllTable2NumMappings(crdbAddrsSlice []string, tableName2NumMapping map[string]int32) {

	// populate CRDB nodes with table numbers
	// connect to CRDB
	crdbAddresses := crdbAddrsSlice
	crdbWrappers := make([]Wrapper, len(crdbAddresses))
	for i, crdbAddr := range crdbAddresses {
		log.Printf("jenndebug crdbAddr [%s]\n", crdbAddr)
		crdbWrappers[i] = Wrapper{
			Addr: crdbAddr,
		}
		grpcConnect(&crdbWrappers[i])
	}
	crdbClients := make([]smdbrpc.HotshardGatewayClient, len(crdbWrappers))
	for i, wrapper := range crdbWrappers {
		crdbClients[i] = wrapper.Client
	}

	var wg2 sync.WaitGroup
	for i := 0; i < len(crdbAddrsSlice); i++ {
		wg2.Add(1)
		go func(idx int) {
			defer wg2.Done()
			populateCRDBTableName2NumMapping(crdbAddrsSlice[idx], crdbClients[idx], tableName2NumMapping)
		}(i)
	}
	wg2.Wait()

}

func filterKeys(tableSet map[string]bool, tableName2NumMapping map[string]int32) []Key {
	keys := make([]Key, 0)
	for _, key := range DATA {
		if _, exists := tableSet[key.TableName]; exists {
			key.TableNum = tableName2NumMapping[key.TableName]
			key.ByteKey[0] = byte(136 + tableName2NumMapping[key.TableName])
			key.ByteValue[8] = byte(136 + tableName2NumMapping[key.TableName])
			keys = append(keys, key)
		} else {
			continue
		}
	}
	return keys
}

func main() {

	batch := flag.Int("batch", 1,
		"number of keys to promote in a single batch")
	cicadaAddr := flag.String("cicadaAddr", "node-11:50051",
		"cicada host machine")
	crdbAddrs := flag.String("crdbAddrs", "node-8:50055,node-9:50055",
		"csv of crdb addresses")
	csvmappingfile := flag.String("csvmappingfile", "", "csv mapping file, maps tables to nums")
	//warehouses := flag.Int("warehouses", 1, "number of warehouses")
	flag.Parse()

	crdbAddrsSlice := strings.Split(*crdbAddrs, ",")

	log.Printf("batch %d, cicadaAddr %s, crdbAddrs %+s\n", *batch, *cicadaAddr,
		crdbAddrsSlice)

	ingest_tpcc_data()

	walltime := time.Now().UnixNano()
	var logical int32 = 0

	tic := time.Now()

	// read the mapping file
	tableName2NumMapping := read_csv_mapping_file(*csvmappingfile)

	// populate CRDB nodes with said mapping
	populateAllTable2NumMappings(crdbAddrsSlice, tableName2NumMapping)

	// key generation--just promote the warehouse table for now
	//index := 1
	//warehouseKeys := deduceWarehouseKeys(tableName2NumMapping[WAREHOUSE], index, *warehouses)
	//districtKeys := deduceDistrictKeys(tableName2NumMapping[DISTRICT], index, *warehouses)
	//keys := append(warehouseKeys, districtKeys...)
	dontcare := true
	tableSet := map[string]bool{
		WAREHOUSE: dontcare,
		ORDER:     dontcare,
	}
	keys := filterKeys(tableSet, tableName2NumMapping)

	// promote keys to both CockroachDB and Cicada.
	promoteKeys(keys, *batch, walltime, logical, *cicadaAddr, crdbAddrsSlice)

	toc := time.Since(tic)
	log.Printf("elapsed %+v\n", toc)
}
