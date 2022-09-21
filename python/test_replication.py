import unittest

import grpc

import smdbrpc_pb2
import smdbrpc_pb2_grpc


class TestReplication(unittest.TestCase):

    def setUp(self) -> None:
        self.num_threads = 12
        self.base_port = 50060

    def test_basic_replication(self):

        for i in range(self.num_threads):
            port = self.base_port + i
            channel = grpc.insecure_channel("localhost:{}".format(port))
            stub = smdbrpc_pb2_grpc.HotshardGatewayStub(channel)

            req = smdbrpc_pb2.ReplicateLogReq()
            resp = stub.ReplicateLog(req)

            self.assertTrue(resp.areReplicated)

    def test_query_thread_meta(self):

        channel = grpc.insecure_channel("localhost:50051")
        stub = smdbrpc_pb2_grpc.HotshardGatewayStub(channel)

        req = smdbrpc_pb2.QueryThreadMetasReq(
            include_global_watermark=True,
            include_watermark=True,
            include_logs=True,
        )
        resp = stub.QueryThreadMetas(req)

        self.assertEqual(self.num_threads, len(resp.thread_metas))



if __name__ == '__main__':
    unittest.main()
