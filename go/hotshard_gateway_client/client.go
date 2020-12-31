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
	"os"
	"strconv"
	"time"

	"google.golang.org/grpc"
	smdbrpc "smdbrpc/go/build/gen"
)

const (
	address = "localhost:50051"
)

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

	var walltime int64 = 20201229
	request := smdbrpc.HotshardRequest{
		Hlctimestamp: &smdbrpc.HLCTimestamp{
			Walltime: &walltime,
		},
	}

	for i := 0; i < len(os.Args); i++ {
		temp, _ := strconv.Atoi(os.Args[i])
		var key = uint64(temp)
		temp, _ = strconv.Atoi(os.Args[i])
		var value = uint64(temp)

		kvPair := smdbrpc.KVPair{
			Key:   &key,
			Value: &value,
		}
		request.WriteKeyset = append(request.WriteKeyset, &kvPair)
		request.ReadKeyset = append(request.ReadKeyset, key)
	}

	//var jennbday, christmas uint64 = 1994214, 1225

	r, err := c.ContactHotshard(
		ctx, &request,
		//&smdbrpc.HotshardRequest{
		//	Hlctimestamp: &smdbrpc.HLCTimestamp{
		//		Walltime: &walltime,
		//	},
		//	WriteKeyset: []*smdbrpc.KVPair{
		//		{
		//			Key:   &jennbday,
		//			Value: &jennbday,
		//		}, {
		//			Key:   &christmas,
		//			Value: &christmas,
		//		},
		//	},
		//	ReadKeyset: []uint64{1994214, 1225},
		//},
	)
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	log.Printf("Greeting:[%+v]\n", *r.IsCommitted)
	for _, kvPair := range r.ReadValueset {
		log.Printf("key:[%+v], val:[%+v]\n",
			*kvPair.Key, *kvPair.Value)
	}
}
