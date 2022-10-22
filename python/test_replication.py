import unittest

import time

import grpc

import smdbrpc_pb2
import smdbrpc_pb2_grpc


class TestReplication(unittest.TestCase):

    def setUp(self) -> None:
        self.num_threads = 12
        self.base_port = 60061
        self.now = time.time_ns()

    def test_basic_replication(self):

        stubs = []
        txnreq = smdbrpc_pb2.ReplicateLogReq(
            txns=[smdbrpc_pb2.TxnReq(
                ops=[smdbrpc_pb2.Op(
                    cmd=smdbrpc_pb2.PUT,
                    table=53,
                    index=1,
                    key="hello".encode(),
                    value="hello".encode(),
                    tableName="warehouse",
                )], timestamp=smdbrpc_pb2.HLCTimestamp(
                    walltime=self.now,
                    logicaltime=0,
                )
            ), smdbrpc_pb2.TxnReq(
                ops=[smdbrpc_pb2.Op(
                    cmd=smdbrpc_pb2.PUT,
                    table=53,
                    index=1,
                    key="world".encode(),
                    value="world".encode(),
                    tableName="warehouse",
                )], timestamp=smdbrpc_pb2.HLCTimestamp(
                    walltime=self.now,
                    logicaltime=1994,
                )
            )],
            currentRT=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now,
                logicaltime=0,
            )
        )
        for i in range(self.num_threads):
            port = self.base_port + i
            destination = "localhost:{}".format(port)
            print(destination)
            channel = grpc.insecure_channel(destination)
            stub = smdbrpc_pb2_grpc.HotshardGatewayStub(channel)
            stubs.append(stub)
            _ = stub.ReplicateLog(txnreq)

        for i in range(self.num_threads):
            req = smdbrpc_pb2.QueryBackupMetaReq()
            resp = stubs[i].QueryBackupMeta(req)
            self.assertEqual(len(txnreq.txns), resp.num_pending_txns)

        # for i in range(self.num_threads):
        #     req = smdbrpc_pb2.QueryThreadMetasReq(
        #         include_global_watermark=False,
        #         include_watermarks=False,
        #         include_logs=True,
        #     )
        #     print("hello", i)
        #     resp = stubs[i].QueryThreadMetas(req)
        #     print("goodbye", i)
        #
        #     self.assertEqual(1, len(resp.thread_metas))
        #
        #     thread_meta = resp.thread_metas[0]
        #     self.assertEqual(len(req.txns), len(thread_meta.log))

    def test_query_thread_meta(self):
        channel = grpc.insecure_channel("localhost:50051")
        stub = smdbrpc_pb2_grpc.HotshardGatewayStub(channel)

        req = smdbrpc_pb2.QueryThreadMetasReq(
            include_global_watermark=True,
            include_watermarks=True,
            include_logs=True,
        )
        resp = stub.QueryThreadMetas(req)

        self.assertEqual(self.num_threads, len(resp.thread_metas))


if __name__ == '__main__':
    unittest.main()
