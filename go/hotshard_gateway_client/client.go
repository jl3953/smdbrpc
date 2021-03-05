/*
 *
 * Copyright 2015 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

// Package main implements a client for Greeter service.
package main

import (
	"context"
	"log"
	"time"

	"google.golang.org/grpc"
	smdbrpc "smdbrpc/go/build/gen"
)

const (
	address = "localhost:50051"
)

func makeRequest(writeset []*smdbrpc.KVPair,
	readset []uint64,
	walltime_ptr *int64,
	logicaltime_ptr *int32) smdbrpc.HotshardRequest {

	request := smdbrpc.HotshardRequest{
		Hlctimestamp: &smdbrpc.HLCTimestamp{
			Walltime:    walltime_ptr,
			Logicaltime: logicaltime_ptr,
		},
		WriteKeyset: writeset,
		ReadKeyset:  readset,
	}

	return request
}

func makeKVPair(key_ptr *uint64, val_ptr *uint64) smdbrpc.KVPair {
	return smdbrpc.KVPair{
		Key:   key_ptr,
		Value: val_ptr,
	}
}

func main() {
	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := smdbrpc.NewHotshardGatewayClient(conn)

	// Contact the server and print out its response.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var walltime int64 = time.Now().UnixNano()
	var logicaltime int32 = 0 /* jenndebug this might be a problem */

	var key, val uint64 = 1994214, 1994214
	kvpair := makeKVPair(&key, &val)
	writeset := []*smdbrpc.KVPair{&kvpair}

	log.Printf("=========CASE 1=========\n")
	/* Case 1:
	txn1: write(1994214) = 1994214, ts = now
	txn2: write(1994214) = 2020, ts = now + 1
	txn3: read(1994214) = 2020, ts = now + 2
	*/
	request := makeRequest(writeset, nil, &walltime, &logicaltime)
	r, err := c.ContactHotshard(ctx, &request)
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	} else if *r.IsCommitted == false {
		log.Printf("isCommitted:[%+v]\n", *r.IsCommitted)
	}

	walltime2 := walltime + 1
	val = 2020
	kvpair = makeKVPair(&key, &val)
	writeset = []*smdbrpc.KVPair{&kvpair}
	request = makeRequest(writeset, nil, &walltime2, &logicaltime)
	r, err = c.ContactHotshard(ctx, &request)
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	} else if *r.IsCommitted == false {
		log.Printf("isCommitted:[%+v]\n", *r.IsCommitted)
	}

	walltime3 := walltime + 2
	readset := []uint64{key}
	request = makeRequest(nil, readset, &walltime3, &logicaltime)
	r, err = c.ContactHotshard(ctx, &request)
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	} else if *r.IsCommitted == false {
		log.Printf("isCommitted:[%+v]\n", *r.IsCommitted)
	}
	for _, kvPair := range r.ReadValueset {
		log.Printf("key:[%+v], val:[%+v]\n",
			*kvPair.Key, *kvPair.Value)
	}
	log.Printf("expected: (1994214, 2020)\n")

	log.Printf("=========CASE 2========\n")
	/* Case 2:
	txn1: write(1994214) = 1994214, ts = now
	txn2: write(1994214) = 2020, ts = now + 5
	txn3: read(1994214) = 1994214, ts = now + 2
	*/
	walltime, logicaltime = time.Now().UnixNano(), 0
	key, val = 1994214, 1994214
	kvpair = makeKVPair(&key, &val)
	writeset = []*smdbrpc.KVPair{&kvpair}
	request = makeRequest(writeset, nil, &walltime, &logicaltime)
	r, err = c.ContactHotshard(ctx, &request)
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	} else if *r.IsCommitted == false {
		log.Printf("isCommitted:[%+v]\n", *r.IsCommitted)
	}

	walltime2 = walltime + 5
	val = 2020
	kvpair = makeKVPair(&key, &val)
	writeset = []*smdbrpc.KVPair{&kvpair}
	request = makeRequest(writeset, nil, &walltime2, &logicaltime)
	r, err = c.ContactHotshard(ctx, &request)
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	} else if *r.IsCommitted == false {
		log.Printf("isCommitted:[%+v]\n", *r.IsCommitted)
	}

	walltime3 = walltime + 2
	readset = []uint64{key}
	request = makeRequest(nil, readset, &walltime3, &logicaltime)
	r, err = c.ContactHotshard(ctx, &request)
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	} else if *r.IsCommitted == false {
		log.Printf("isCommitted:[%+v]\n", *r.IsCommitted)
	}
	for _, kvPair := range r.ReadValueset {
		log.Printf("key:[%+v], val:[%+v]\n",
			*kvPair.Key, *kvPair.Value)
	}
	log.Printf("expected: (1994214, 1994214)\n")

	log.Printf("=========CASE 3=========\n")
	/* Case 3:
	txn1: write(1994214) = 1994214, ts = now
	txn2: write(1994214) = 2020, ts = now - 10 FAIL
	txn3: read(1994214) = 1994214, ts = now + 1
	*/
	walltime, logicaltime = time.Now().UnixNano(), 0
	key, val = 1994214, 1994214
	kvpair = makeKVPair(&key, &val)
	writeset = []*smdbrpc.KVPair{&kvpair}
	request = makeRequest(writeset, nil, &walltime, &logicaltime)
	r, err = c.ContactHotshard(ctx, &request)
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	} else if *r.IsCommitted == false {
		log.Printf("isCommitted:[%+v]\n", *r.IsCommitted)
	}

	walltime2 = walltime - 10
	val = 2020
	kvpair = makeKVPair(&key, &val)
	writeset = []*smdbrpc.KVPair{&kvpair}
	request = makeRequest(writeset, nil, &walltime2, &logicaltime)
	r, err = c.ContactHotshard(ctx, &request)
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	log.Printf("isCommitted:[%+v]\n", *r.IsCommitted)
	log.Printf("EXPECTED: false\n")

	walltime3 = walltime + 2
	readset = []uint64{key}
	request = makeRequest(nil, readset, &walltime3, &logicaltime)
	r, err = c.ContactHotshard(ctx, &request)
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	} else if *r.IsCommitted == false {
		log.Printf("isCommitted:[%+v]\n", *r.IsCommitted)
	}
	for _, kvPair := range r.ReadValueset {
		log.Printf("key:[%+v], val:[%+v]\n",
			*kvPair.Key, *kvPair.Value)
	}
	log.Printf("expected: (1994214, 1994214)\n")

	log.Printf("=========CASE 4=========\n")
	/* Case 4:
	txn1: write(1994214) = 1994214, ts = now, 0
	txn2: write(1994214) = 2020, ts = now, 1
	txn3: read(1994214) = 2020, ts = now, 2
	*/
	walltime, logicaltime = time.Now().UnixNano(), 0
	request = makeRequest(writeset, nil, &walltime, &logicaltime)
	r, err = c.ContactHotshard(ctx, &request)
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	} else if *r.IsCommitted == false {
		log.Printf("isCommitted:[%+v]\n", *r.IsCommitted)
	}

	logicaltime2 := logicaltime + 1
	val = 2020
	kvpair = makeKVPair(&key, &val)
	writeset = []*smdbrpc.KVPair{&kvpair}
	request = makeRequest(writeset, nil, &walltime, &logicaltime2)
	r, err = c.ContactHotshard(ctx, &request)
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	} else if *r.IsCommitted == false {
		log.Printf("isCommitted:[%+v]\n", *r.IsCommitted)
	}

	logicaltime3 := logicaltime + 2
	readset = []uint64{key}
	request = makeRequest(nil, readset, &walltime, &logicaltime3)
	r, err = c.ContactHotshard(ctx, &request)
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	} else if *r.IsCommitted == false {
		log.Printf("isCommitted:[%+v]\n", *r.IsCommitted)
	}
	for _, kvPair := range r.ReadValueset {
		log.Printf("key:[%+v], val:[%+v]\n",
			*kvPair.Key, *kvPair.Value)
	}
	log.Printf("expected: (1994214, 2020)\n")

	log.Printf("=========CASE 5=========\n")
	/* Case 5:
	txn1: write(1994214) = 1994214, ts = now, 0
	txn2: write(1994214) = 2020, ts = now, 10
	txn3: read(1994214) = 1994214, ts = now, 5
	*/
	walltime, logicaltime = time.Now().UnixNano(), 0
	key, val = 1994214, 1994214
	kvpair = makeKVPair(&key, &val)
	writeset = []*smdbrpc.KVPair{&kvpair}
	request = makeRequest(writeset, nil, &walltime, &logicaltime)
	r, err = c.ContactHotshard(ctx, &request)
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	} else if *r.IsCommitted == false {
		log.Printf("isCommitted:[%+v]\n", *r.IsCommitted)
	}

	logicaltime2 = logicaltime + 10
	val = 2020
	kvpair = makeKVPair(&key, &val)
	writeset = []*smdbrpc.KVPair{&kvpair}
	request = makeRequest(writeset, nil, &walltime, &logicaltime2)
	r, err = c.ContactHotshard(ctx, &request)
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	} else if *r.IsCommitted == false {
		log.Printf("isCommitted:[%+v]\n", *r.IsCommitted)
	}

	logicaltime3 = logicaltime + 5
	readset = []uint64{key}
	request = makeRequest(nil, readset, &walltime, &logicaltime3)
	r, err = c.ContactHotshard(ctx, &request)
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	} else if *r.IsCommitted == false {
		log.Printf("isCommitted:[%+v]\n", *r.IsCommitted)
	}
	for _, kvPair := range r.ReadValueset {
		log.Printf("key:[%+v], val:[%+v]\n",
			*kvPair.Key, *kvPair.Value)
	}
	log.Printf("expected: (1994214, 1994214)\n")

	log.Printf("=========CASE 6=========\n")
	/* Case 6:
	txn1: write(1994214) = 1994214, ts = now, 100
	txn2: write(1994214) = 2020, ts = now, 90 FAIL
	txn3: read(1994214) = 1994214, ts = now, 101
	*/
	walltime, logicaltime = time.Now().UnixNano(), 100
	key, val = 1994214, 1994214
	kvpair = makeKVPair(&key, &val)
	writeset = []*smdbrpc.KVPair{&kvpair}
	request = makeRequest(writeset, nil, &walltime, &logicaltime)
	r, err = c.ContactHotshard(ctx, &request)
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	} else if *r.IsCommitted == false {
		log.Printf("isCommitted:[%+v]\n", *r.IsCommitted)
	}

	logicaltime2 = logicaltime - 10
	val = 2020
	kvpair = makeKVPair(&key, &val)
	writeset = []*smdbrpc.KVPair{&kvpair}
	request = makeRequest(writeset, nil, &walltime, &logicaltime2)
	r, err = c.ContactHotshard(ctx, &request)
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	log.Printf("isCommitted:[%+v]\n", *r.IsCommitted)
	log.Printf("EXPECTED: false\n")

	logicaltime3 = logicaltime + 2
	readset = []uint64{key}
	request = makeRequest(nil, readset, &walltime, &logicaltime3)
	r, err = c.ContactHotshard(ctx, &request)
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	} else if *r.IsCommitted == false {
		log.Printf("isCommitted:[%+v]\n", *r.IsCommitted)
	}
	for _, kvPair := range r.ReadValueset {
		log.Printf("key:[%+v], val:[%+v]\n",
			*kvPair.Key, *kvPair.Value)
	}
	log.Printf("expected: (1994214, 1994214)\n")
}
