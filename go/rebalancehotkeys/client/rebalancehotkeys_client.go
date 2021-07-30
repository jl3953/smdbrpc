package main

import (
	"context"
	"google.golang.org/grpc"
	"log"
	execinfrapb "smdbrpc/go/build/gen"
	"time"
)

func main() {

	conn, err := grpc.Dial("localhost:50055",
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
		log.Printf("CRDB key %d, qps %d, write_qps %d\n",
			crdbKeyStat.GetKey(), crdbKeyStat.GetQps(),
			crdbKeyStat.GetWriteQps())
	}

	conn, err = grpc.Dial("localhost:50054",
		grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect %+v", err)
	}
	defer func(conn *grpc.ClientConn) {
		_ = conn.Close()
	}(conn)
	c = execinfrapb.NewRebalanceHotkeysGatewayClient(conn)

	cicadaR, err := c.RequestCicadaStats(ctx, &request)
	if err != nil {
		log.Fatalf("could not request CRDB keystats %+v", err)
	}
	for _, cicadaStat := range cicadaR.Keystats {
		log.Printf("cicada key %d, qps %d, write_qps %d\n",
			cicadaStat.GetKey(), cicadaStat.GetQps(),
			cicadaStat.GetWriteQps())
	}
	log.Printf("cicada cpu usage %d, mem usage %d",
		cicadaR.GetCpuusage(), cicadaR.GetMemusage())
}
