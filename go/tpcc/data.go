package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

const (
	WAREHOUSE = "warehouse"
)

type Key struct {
	TableName string
	TableNum  int32
	Index     int
	PkCols    []int64
	ByteKey   []byte
}

func (key *Key) hash() string {
	result := key.TableName
	for _, pkCol := range key.PkCols {
		result += strconv.Itoa(int(pkCol))
	}
	return result
}

var WAREHOUSE_KEYS []Key
var DATA = map[string][]byte{}

func ingest_tpcc_data() {
	cur_dir, _ := os.Getwd()
	datafile := cur_dir + "/tpcc/tpcc_data.txt"
	fd, err := os.Open(datafile)
	if err != nil {
		fmt.Printf("Opening %s error %+v\n", datafile, err)
	}
	defer func(fd *os.File) {
		err := fd.Close()
		if err != nil {

		}
	}(fd)

	fileScanner := bufio.NewScanner(fd)
	fileScanner.Split(bufio.ScanLines)
	for fileScanner.Scan() {
		line := fileScanner.Text()
		data := strings.Split(line, ":")

		// handle table information
		tableMeta := strings.Split(strings.TrimSpace(data[0]), ",")
		tableName := tableMeta[0]
		pkCols := make([]int64, 0)
		for i := 1; i < len(tableMeta); i++ {
			pkCol, _ := strconv.Atoi(strings.TrimSpace(tableMeta[i]))
			pkCols = append(pkCols, int64(pkCol))
		}
		key := Key{
			TableName: tableName,
			PkCols:    pkCols,
		}

		// populate warehouse keys
		WAREHOUSE_KEYS = append(WAREHOUSE_KEYS, key)

		// handle data
		byteDataAsStrings := strings.Split(strings.TrimSpace(data[1]), " ")
		var byteData []byte
		for _, byteDatumAsString := range byteDataAsStrings {
			byteDatum, _ := strconv.Atoi(byteDatumAsString)
			byteData = append(byteData, byte(byteDatum))
		}

		// populate data
		DATA[key.hash()] = byteData
	}

	for key, val := range DATA {
		fmt.Printf("%s, %+v\n", key, val)
	}
}

//var WAREHOUSE_KEYS = []Key{
//	{
//		TableName: WAREHOUSE,
//		PkCols:    []int64{0},
//	}, {
//		TableName: WAREHOUSE,
//		PkCols:    []int64{1},
//	}, {
//		TableName: WAREHOUSE,
//		PkCols:    []int64{2},
//	},
//}
//
//var DATA = map[string][]byte{
//	WAREHOUSE_KEYS[1].hash(): []byte{48, 0, 0, 0, 14, 0, 0, 0, 189, 137, 137, 136, 0, 23, 82, 151,
//		8, 105, 47, 38, 122, 9, 254, 165, 152, 167, 10, 38, 1, 57, 22, 2,
//		49, 57, 22, 2, 50, 48, 22, 2, 49, 57, 22, 2, 78, 80, 22, 9,
//		54, 52, 48, 57, 49, 49, 49, 49, 49, 21, 3, 41,
//		7, 116, 21, 6, 52, 142, 1, 201, 195, 128},
//}
