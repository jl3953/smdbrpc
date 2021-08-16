package main

import (
	"context"
	"google.golang.org/grpc"
	"log"
	"net"
	execinfrapb "smdbrpc/go/build/gen"
)

func (s *server) RequestCRDBKeyStats(_ context.Context,
	_ *execinfrapb.KeyStatsRequest) (*execinfrapb.CRDBKeyStatsResponse, error) {

	var jennbday, jeffbday uint64 = 214, 812
	response := execinfrapb.CRDBKeyStatsResponse{
		Keystats: []*execinfrapb.KeyStat{
			{
				Key:      &jennbday,
				Qps:      &jennbday,
				WriteQps: &jennbday,
			}, {
				Key:      &jeffbday,
				Qps:      &jeffbday,
				WriteQps: &jeffbday,
			},
		},
	}

	return &response, nil
}

func (s *server) RequestCicadaStats(_ context.Context,
	_ *execinfrapb.KeyStatsRequest) (*execinfrapb.CicadaStatsResponse, error) {

	// the go server cannot implement this
	return nil, nil
}

type server struct {
	execinfrapb.UnimplementedHotshardGatewayServer
}

func main() {
	lis, err := net.Listen("tcp", ":50055")
	if err != nil {
		log.Fatalf("failed to listen %+v\n", err)
	}
	s := grpc.NewServer()
	execinfrapb.RegisterHotshardGatewayServer(s, &server{})
	log.Printf("rebalancehotkeys serving!")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve rebalancehotkeys %+v\n", err)
	}
}
