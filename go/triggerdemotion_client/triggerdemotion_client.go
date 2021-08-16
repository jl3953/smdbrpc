package main

import (
	"context"
	"google.golang.org/grpc"
	"log"
	smdbrpc "smdbrpc/go/build/gen"
	"time"
)

const(
	address = "localhost: 50051"
)

func main() {

	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer func(conn *grpc.ClientConn) {
		_ = conn.Close()
	}(conn)
	c := smdbrpc.NewHotshardGatewayClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	request := smdbrpc.TriggerDemotionRequest{
		Keys: []uint64{1994214, 1994812},
	}

	r, err := c.TriggerDemotion(ctx, &request)
	if err != nil {
		log.Fatalf("could not TriggerDemotion, err %+v", err)
	}
	for _, triggerDemotionStatus := range r.TriggerDemotionStatuses {
		log.Printf("key %+v, status %+v\n",
			triggerDemotionStatus.GetKey(), triggerDemotionStatus.GetIsDemotionTriggered())
	}

}
