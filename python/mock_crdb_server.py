import logging

import grpc

import smdbrpc_pb2
import smdbrpc_pb2_grpc

from concurrent import futures


class MockCRDBServer(smdbrpc_pb2_grpc.HotshardGatewayServicer):

    def UpdatePromotionMap(self, request, context):

        reply = smdbrpc_pb2.PromoteKeysResp(
            were_successfully_migrated=[],
        )
        for promotedKey in request.keys:
            print("promotedKey.cicada_key_cols", promotedKey.cicada_key_cols)
            reply.were_successfully_migrated.append(
                smdbrpc_pb2.KeyMigrationResp(
                    is_successfully_migrated=True,
                )
            )

        return reply


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    smdbrpc_pb2_grpc.add_HotshardGatewayServicer_to_server(
        MockCRDBServer(), server
    )
    server.add_insecure_port('localhost:50055')
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    logging.basicConfig()
    serve()
