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
	"google.golang.org/grpc"
	"log"
	"os"
	"reflect"
	pb "smdbrpc/go/build/gen"
	"time"
)

const (
	address          = "localhost:50051"
	defaultSQLString = "UPSERT INTO TABLE hot (0, 1994214)"
)

func getSize(v interface{}) int {
	size := int(reflect.TypeOf(v).Size())
	switch reflect.TypeOf(v).Kind() {
	case reflect.Slice:
		s := reflect.ValueOf(v)
		for i := 0; i < s.Len(); i++ {
			size += getSize(s.Index(i).Interface())
		}
	case reflect.Map:
		s := reflect.ValueOf(v)
		keys := s.MapKeys()
		size += int(float64(len(keys)) * 10.79) // approximation from https://golang.org/src/runtime/hashmap.go
		for i := range keys {
			size += getSize(keys[i].Interface()) + getSize(s.MapIndex(keys[i]).Interface())
		}
	case reflect.String:
		size += reflect.ValueOf(v).Len()
	case reflect.Struct:
		s := reflect.ValueOf(v)
		for i := 0; i < s.NumField(); i++ {
			if s.Field(i).CanInterface() {
				size += getSize(s.Field(i).Interface())
			}
		}
	}
	return size
}

func main() {
	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewHotshardGatewayClient(conn)

	// Contact the server and print out its response.
	sqlString := defaultSQLString
	if len(os.Args) > 1 {
		sqlString = os.Args[1]
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	hotshardRequest := pb.HotshardRequest{
		Sqlstring: sqlString,
		Hlctimestamp: &pb.HLCTimestamp{
			Walltime:    1994,
			Logicaltime: 214,
		},
	}
	r, err := c.ContactHotshard(ctx, &hotshardRequest)
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	log.Printf("sizeof(hotshardRequest):[%+v], sizeof(hotshardReply):[%+v]\n",
		getSize(hotshardRequest), getSize(*r))
	log.Printf("Reply: r.GetIsCommitted:[%+v], r.GetHlctimestamp:[%+v]\n",
		r.GetIsCommitted(), r.GetHlctimestamp())
}
