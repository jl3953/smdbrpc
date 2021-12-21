package main

import (
	"crypto/sha256"
	"encoding/binary"
	"log"
)

func randomizeHash(key int64, keyspace int64) int64 {
	byteKey := make([]byte, 8)
	binary.BigEndian.PutUint64(byteKey, uint64(key))
	hashed32Bytes := sha256.Sum256(byteKey)
	hashed := make([]byte, 32)
	for i, b := range hashed32Bytes {
		hashed[i] = b
	}
	hashedUint64 := binary.BigEndian.Uint64(hashed)
	hashedModulo := hashedUint64 % uint64(keyspace + 1)
	return int64(hashedModulo)
}
func main() {

	zero := randomizeHash(0, 1000000)
	oneM := randomizeHash(1000000, 1000000)

	log.Printf("zero %d, 1M %d\n", zero, oneM)
}
