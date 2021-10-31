import logging

import grpc

import smdbrpc_pb2
import smdbrpc_pb2_grpc

from concurrent import futures


class MockCicadaServer(smdbrpc_pb2_grpc.HotshardGatewayServicer):

    def CalculateCicadaStats(self, request, context):
        # Test whether demotion only triggers
        # reply = smdbrpc_pb2.CalculateCicadaStatsResp(
        #     demotion_only=True,
        #     qps_avail_for_promotion=1000,
        #     num_keys_avail_for_promotion=1000,
        #     qps_at_nth_percentile=50.0,
        # )

        # Test whether promotion only triggers
        reply = smdbrpc_pb2.CalculateCicadaStatsResp(
            demotion_only=False,
            qps_avail_for_promotion=90,
            num_keys_avail_for_promotion=1000,
        )

        # Test whether reorg triggers correctly
        # reply = smdbrpc_pb2.CalculateCicadaStatsResp(
        #     demotion_only=False,
        #     qps_avail_for_promotion=0,
        #     num_keys_avail_for_promotion=0,
        #     qps_at_nth_percentile=50,
        # )
        return reply

    def SendTxn(self, request, context):
        print("jenndebug ops", request.ops)
        print("jenndebug timestamp", request.timestamp)
        print("jenndebug isPromotion", request.is_promotion)
        reply = smdbrpc_pb2.TxnResp(
            is_committed=True,
        )
        return reply


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    smdbrpc_pb2_grpc.add_HotshardGatewayServicer_to_server(MockCicadaServer(), server)
    server.add_insecure_port('localhost:50051')
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    logging.basicConfig()
    serve()