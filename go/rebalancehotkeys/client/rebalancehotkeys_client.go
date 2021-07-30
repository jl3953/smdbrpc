package main

import (
	"context"
	"google.golang.org/grpc"
	"log"
	execinfrapb "smdbrpc/go/build/gen"
	"time"
)

func main() {

	conn, err := grpc.Dial("localhost:50054",
		grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect %+v", err)
	}
	defer func(conn *grpc.ClientConn) {
		_ = conn.Close()
	}(conn)
	c := execinfrapb.NewRebalanceHotkeysGatewayClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	request := execinfrapb.KeyStatsRequest{}

	r, err := c.RequestCRDBKeyStats(ctx, &request)
	if err != nil {
		log.Fatalf("could not request CRDB keystats %+v", err)
	}
	for _, crdbKeyStat := range r.Keystats {
		log.Printf("key %d, qps %d, write_qps %d\n",
			crdbKeyStat.GetKey(), crdbKeyStat.GetQps(),
			crdbKeyStat.GetWriteQps())
	}
}
