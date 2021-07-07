package main

import (
	"context"
	"google.golang.org/grpc"
	"log"
	"net"

	demotehotkeys "smdbrpc/go/build/gen"
)

func (s *server) demoteHotkeys(
	_ context.Context,
	in *demotehotkeys.DemoteHotkeysRequest) (*demotehotkeys.DemoteHotkeysReply, error) {

	log.Printf("Received %+v", in.String())
	key := "jennifer"
	isSuccessfullyDemoted := true
	return &demotehotkeys.DemoteHotkeysReply{
		AreSuccessfullyDemoted: []*demotehotkeys.KVDemotionStatus{
			{
				Key:                   &key,
				IsSuccessfullyDemoted: &isSuccessfullyDemoted,
			},
		},
	}, nil

}

type server struct {
	demotehotkeys.UnimplementedDemoteHotkeysGatewayServer
}

func main() {
	lis, err := net.Listen("tcp", ":50052")
	if err != nil {
		log.Fatalf("failed to listen: %+v", err)
	}
	s := grpc.NewServer()
	demotehotkeys.RegisterDemoteHotkeysGatewayServer(
		s,
		&server{},
	)
	log.Printf("serving!")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to server %+v", err)
	}

}
