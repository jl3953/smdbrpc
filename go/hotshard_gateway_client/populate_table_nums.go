package main

import (
	"context"
	"flag"
	"fmt"
	"google.golang.org/grpc"
	"log"
	smdbrpc "smdbrpc/go/build/gen"
	"strings"
	"sync"
	"time"
)

type CRDBWrapper struct {
	Addr    string
	ConnPtr *grpc.ClientConn
	Client  smdbrpc.HotshardGatewayClient
}

func grpcCRDBConnect(wrapper *CRDBWrapper) {
	var err error
	wrapper.ConnPtr, err = grpc.Dial(wrapper.Addr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("failed to dial %+v\n", wrapper.Addr)
	}
	wrapper.Client = smdbrpc.NewHotshardGatewayClient(wrapper.ConnPtr)
}

func queryTableNumMapping(_ smdbrpc.HotshardGatewayClient) (
	mapping map[string]int32) {

	mapping = map[string]int32{
		"warehouse": 53,
		"district": 54,
		"customer": 55,
		"history": 56,
		"order": 57,
		"new_order": 58,
		"item": 59,
		"stock": 60,
		"order_line": 61,
	}

	//ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	//defer cancel()
	//
	//req := smdbrpc.QueryTableNumFromNameReq{TableNames: []string{"warehouse",
	//	"district", "customer", "item", "stock", "history", "neworder",
	//	"order", "orderline"}}
	//resp, err := client.QueryTableNumFromName(ctx, &req)
	//if err != nil {
	//	log.Fatalf("jenndebug QueryTableNum rpc call failed %+v\n", err)
	//}
	//
	//for _, tableNumMapping := range resp.GetMapping() {
	//	mapping[tableNumMapping.GetName()] = tableNumMapping.GetNum()
	//}

	return mapping
}


func populateCRDBTableNumMapping(mapping map[string]int32, client smdbrpc.
	HotshardGatewayClient) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	req := smdbrpc.PopulateCRDBTableNumMappingReq{
		TableNumMappings: make([]*smdbrpc.TableNumMapping, 0),
	}
	for name, num := range mapping {
		localName := name
		localNum := num
		fmt.Printf("name [%s], num:[%d]\n", localName, localNum)
		req.TableNumMappings = append(req.TableNumMappings, &smdbrpc.TableNumMapping{
			TableName: &localName,
			TableNum:  &localNum,
		})
	}
	resp, err := client.PopulateCRDBTableNumMapping(ctx, &req)
	if err != nil {
		log.Fatalf("jenndebug rpc to populate crdb table num mappings failed"+
			" %+v\n", err)

	} else if !*resp.IsPopulated {
		log.Fatalf("jenndebug populate crdb table num mapping did not run" +
			" correctly\n")
	}
}

func main() {
	crdbAddrs := flag.String("crdbAddrs", "localhost:50055",
		"csv of crdb addresses")
	flag.Parse()

	crdbAddrsSlice := strings.Split(*crdbAddrs, ",")

	log.Printf("crdbAddrs %s\n", crdbAddrsSlice)

	// connect to CRDB
	crdbWrappers := make([]CRDBWrapper, len(crdbAddrsSlice))
	for i, crdbAddr := range crdbAddrsSlice {
		crdbWrappers[i] = CRDBWrapper{
			Addr:    crdbAddr,
			ConnPtr: nil,
			Client:  nil,
		}
		grpcCRDBConnect(&crdbWrappers[i])
	}
	crdbClients := make([]smdbrpc.HotshardGatewayClient, len(crdbWrappers))
	for i, wrapper := range crdbWrappers {
		crdbClients[i] = wrapper.Client
	}

	// Query table num mapping
	mapping := queryTableNumMapping(crdbClients[0])

	var wg sync.WaitGroup
	for i := 0; i < len(crdbClients); i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			populateCRDBTableNumMapping(mapping, crdbClients[idx])
		}(i)
	}
	wg.Wait()
}
