from __future__ import print_function
import logging

import grpc

import smdbrpc_pb2
import smdbrpc_pb2_grpc


def run():
    # NOTE(gRPC Python Team): .close() is possible on a channel and should be
    # used in circumstances in which the with statement does not fit the needs
    # of the code.
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = smdbrpc_pb2_grpc.HotshardGatewayStub(channel)
        response = stub.ContactHotshard(smdbrpc_pb2.HotshardRequest(
            hlctimestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=1994214,
                logicaltime=214,
            ),
            write_keyset=[smdbrpc_pb2.KVPair(key=1994214, value=1994214)],
            read_keyset=[1994214],
        ))
        print("Greeter client received:\n", response)


if __name__ == '__main__':
    logging.basicConfig()
    run()
