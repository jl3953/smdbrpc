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

    def test_full_pipeline(self):

        # ping the head
        channel = grpc.insecure_channel("localhost:50051")
        stub = smdbrpc_pb2_grpc.HotshardGatewayStub(channel)

        promotionReq = smdbrpc_pb2.PromoteKeysToCicadaReq(
            keys=[smdbrpc_pb2.Key(
                table=53, tableName="warehouse", index=1,
                cicada_key_cols=[994813], key="promoted".encode(),
                timestamp=smdbrpc_pb2.HLCTimestamp(
                    walltime=self.now, logicaltime=0, ),
                value="promoted".encode(), ), smdbrpc_pb2.Key(
                table=54, tableName="district", index=1,
                cicada_key_cols=[200604, 200604], key="promoted".encode(),
                timestamp=smdbrpc_pb2.HLCTimestamp(
                    walltime=self.now, logicaltime=0, ),
                value="promoted".encode(), ), smdbrpc_pb2.Key(
                table=61, tableName="stock", index=1,
                cicada_key_cols=[200608, 200608], key="promoted".encode(),
                timestamp=smdbrpc_pb2.HLCTimestamp(
                    walltime=self.now, logicaltime=0, ), ), ]
        )
        promotionResp = stub.PromoteKeysToCicada(promotionReq)
        self.assertEqual(
            len(promotionReq.keys), len(
                promotionResp.successfullyPromoted

            )
        )
        for promoted in promotionResp.successfullyPromoted:
            self.assertTrue(promoted)

        self.now = time.time_ns()
        key1 = 994813
        key2 = 200604
        key3 = 220604
        response = stub.BatchSendTxns(
            smdbrpc_pb2.BatchSendTxnsReq(
                txns=[smdbrpc_pb2.TxnReq(
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now + 100, logicaltime=0, ),
                    ops=[smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.PUT, table=53, tableName="warehouse",
                        index=1, cicada_key_cols=[key1], key=str(key1).encode(),
                        value=str(key1).encode(), ), ], ), smdbrpc_pb2.TxnReq(
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now + 100, logicaltime=0, ),
                    ops=[smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.PUT, table=54, tableName="district",
                        index=1, cicada_key_cols=[key2, key2], key=str(key2).encode(),
                        value=str(key2).encode(), ), ], ),
                    smdbrpc_pb2.TxnReq(
                        timestamp=smdbrpc_pb2.HLCTimestamp(
                            walltime=self.now + 100, logicaltime=0, ),
                        ops=[smdbrpc_pb2.Op(
                            cmd=smdbrpc_pb2.PUT, table=61, tableName="stock",
                            index=1, cicada_key_cols=[key3, key3], key=str(key3).encode(),
                            value=str(key3).encode(), ), ], ), ]
            )
        )
        self.assertEqual(3, len(response.txnResps))
        i = 0
        for txnResp in response.txnResps:
            self.assertTrue(txnResp.is_committed)
            print(i)
            i += 1

        self.now = time.time_ns()
        response = stub.BatchSendTxns(
            smdbrpc_pb2.BatchSendTxnsReq(
                txns=[smdbrpc_pb2.TxnReq(
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now + 200, logicaltime=0, ),
                    ops=[smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.PUT, table=53, tableName="warehouse",
                        index=1, cicada_key_cols=[key1],
                        key=str(key1).encode(), value=str(key3).encode(), ), ]
                ), smdbrpc_pb2.TxnReq(
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now + 200, logicaltime=0, ),
                    ops=[smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.PUT, table=54, tableName="district",
                        index=1, cicada_key_cols=[key2, key2],
                        key=str(key2).encode(), value=str(key3).encode())]
                ), smdbrpc_pb2.TxnReq(
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now + 200, logicaltime=0, ),
                    ops=[smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.GET, table=61, tableName="stock",
                        index=1, cicada_key_cols=[key3, key3],
                        key=str(key3).encode(), )]
                ), ]
            )
        )
        self.assertEqual(3, len(response.txnResps))
        for txnResp in response.txnResps:
            self.assertTrue(txnResp.is_committed)

        # check that the messages are at the backup
        num_pending_txns = 0
        for i in range(self.num_threads):
            port = self.base_port + i
            destination = "localhost:{}".format(port)
            print(destination)
            channel = grpc.insecure_channel(destination)
            stub = smdbrpc_pb2_grpc.HotshardGatewayStub(channel)
            req = smdbrpc_pb2.QueryBackupMetaReq()
            resp = stub.QueryBackupMeta(req)
            num_pending_txns += resp.num_pending_txns

        self.assertEqual(5, num_pending_txns)


if __name__ == '__main__':
    unittest.main()
