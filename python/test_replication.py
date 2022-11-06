import unittest

import time
import threading

import grpc

import smdbrpc_pb2
import smdbrpc_pb2_grpc


class TestReplication(unittest.TestCase):

    def setUp(self) -> None:
        self.num_threads = 1
        self.base_port = 60061

        channel = grpc.insecure_channel("localhost:60062")
        self.stub = smdbrpc_pb2_grpc.HotshardGatewayStub(channel)

        promotion_req = smdbrpc_pb2.PromoteKeysToCicadaReq(
            keys=[smdbrpc_pb2.Key(
                table=53, tableName="warehouse", index=1,
                cicada_key_cols=[1, 0], key="promoted".encode(),
                timestamp=smdbrpc_pb2.HLCTimestamp(
                    walltime=time.time_ns(), logicaltime=0, ),
                value="promoted".encode(), ), smdbrpc_pb2.Key(
                table=53, tableName="warehouse", index=1,
                cicada_key_cols=[2, 0], key="promoted".encode(),
                timestamp=smdbrpc_pb2.HLCTimestamp(
                    walltime=time.time_ns(), logicaltime=0, ),
                value="promoted".encode(), ),
            ]
        )

        temp_channel = grpc.insecure_channel("localhost:60061")
        temp_stub = smdbrpc_pb2_grpc.HotshardGatewayStub(temp_channel)

        promotion_resp = temp_stub.PromoteKeysToCicada(promotion_req)
        self.assertEqual(
            len(promotion_req.keys), len(
                promotion_resp.successfullyPromoted
            )
        )
        for promoted in promotion_resp.successfullyPromoted:
            self.assertTrue(promoted)

        self.now = time.time_ns()

    def test_basic_replication(self):

        def replay_on_tail():
            time.sleep(2)
            print("executed")
            _ = self.stub.ReplayOnTail(smdbrpc_pb2.ReplayOnTailReq(
                global_watermark=smdbrpc_pb2.HLCTimestamp(
                    walltime=self.now + 1000000000,
                    logicaltime=0,
                )
            ))

        t = threading.Thread(target=replay_on_tail, args=())
        t.start()

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
                    cicada_key_cols=[1, 0]
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
                    cicada_key_cols=[2, 0]
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
