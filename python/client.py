from __future__ import print_function

import argparse
import logging

import grpc

import smdbrpc_pb2
import smdbrpc_pb2_grpc

import time


def run():

    parser = argparse.ArgumentParser("sends a bunch of requests to a server")
    parser.add_argument("--num_keys", type=int, default=100, help="number of keys to send")
    args = parser.parse_args()
    # NOTE(gRPC Python Team): .close() is possible on a channel and should be
    # used in circumstances in which the with statement does not fit the needs
    # of the code.
    with grpc.insecure_channel('localhost:50051') as channel:
        for i in range(args.num_keys):
            if i % 1000 == 0:
                print("Processed up to", i)
            stub = smdbrpc_pb2_grpc.HotshardGatewayStub(channel)
            response = stub.ContactHotshard(smdbrpc_pb2.HotshardRequest(
                hlctimestamp=smdbrpc_pb2.HLCTimestamp(
                    walltime=time.time_ns(),
                    logicaltime=0,
                ),
                write_keyset=[smdbrpc_pb2.KVPair(key=i, value=i)],
                read_keyset=[],
            ))
            if response.is_committed:
                read_response = stub.ContactHotshard(smdbrpc_pb2.HotshardRequest(
                    hlctimestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=time.time_ns(),
                        logicaltime=1,
                    ),
                    write_keyset=[],
                    read_keyset=[i],
                ))
                if read_response.is_committed:
                    if len(read_response.read_valueset) == 1 and read_response.read_valueset[0].value == i:
                        continue
                    else:
                        print("Read empty on", i)
                        print("Read response", read_response)
                        raise BaseException
                else:
                    print("Read failed to commit on", i)
                    raise BaseException
            else:
                print("Write failed to commit on", i)
                raise BaseException

    print("Successfully wrote all {} keys!".format(args.num_keys))


if __name__ == '__main__':
    logging.basicConfig()
    run()
