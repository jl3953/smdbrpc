package main

import (
	"google.golang.org/grpc"
	"io"
	"log"
	"net"

	demotehotkeys "smdbrpc/go/build/gen"
)

func (s *server) DemoteHotkeys(stream demotehotkeys.DemoteHotkeysGateway_DemoteHotkeysServer) error {
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		key := in.GetKey()
		log.Printf("%+v, %+v, (%+v, %+v), %+v\n",
			key, in.GetValue(), *in.GetTimestamp().Walltime,
			*in.GetTimestamp().Logicaltime, in.GetHotness())

		isSuccessfullyDemoted := true

		demotionStatus := demotehotkeys.KVDemotionStatus{
			Key:                   &key,
			IsSuccessfullyDemoted: &isSuccessfullyDemoted,
		}
		if err := stream.Send(&demotionStatus); err != nil {
			return err
		}
	}
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
