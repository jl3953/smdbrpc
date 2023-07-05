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
	DISTRICT  = "district"
	ORDER     = "order"
)

type Key struct {
	TableName string
	TableNum  int32
	Index     int
	PkCols    []int64
	ByteKey   []byte
	ByteValue []byte
}

func (key *Key) hash() string {
	result := key.TableName
	for _, pkCol := range key.PkCols {
		result += strconv.Itoa(int(pkCol))
	}
	return result
}

var DATA = map[string]Key{}

func shouldStop(byteData []byte, i int) bool {
	return byteData[i] == 136 && byteData[i+1] == 0 && byteData[i+2] == 23
}

func extractKey(byteData []byte) []byte {

	key := make([]byte, 0)
	for i := 8; shouldStop(byteData, i) == false; i++ {
		key = append(key, byteData[i])
	}
	key = append(key, byte(136))
	return key
}

/**
Ingests data from hard coded data file.
*/
func ingestTpccData() {
	curDir, _ := os.Getwd()
	datafile := curDir + "/tpcc/tpcc_data.txt"
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
		for i := 2; i < len(tableMeta); i++ {
			pkCol, _ := strconv.Atoi(strings.TrimSpace(tableMeta[i]))
			pkCols = append(pkCols, int64(pkCol))
		}

		byteDataAsStrings := strings.Split(strings.TrimSpace(data[1]), " ")
		var byteData []byte
		for _, byteDatumAsString := range byteDataAsStrings {
			byteDatum, _ := strconv.Atoi(byteDatumAsString)
			byteData = append(byteData, byte(byteDatum))
		}

		key := Key{
			TableName: tableName,
			TableNum:  int32(byteData[8] - 136),
			Index:     int(byteData[9] - 136),
			PkCols:    pkCols,
			ByteKey:   extractKey(byteData),
			ByteValue: byteData,
		}

		// populate data
		DATA[key.hash()] = key
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
