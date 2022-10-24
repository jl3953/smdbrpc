import unittest

import time
import grpc
import smdbrpc_pb2
import smdbrpc_pb2_grpc


class MyTestCase(unittest.TestCase):

    def setUp(self) -> None:
        self.num_threads = 1
        self.base_port = 60061
        self.now = time.time_ns()

        channel = grpc.insecure_channel("localhost:50051")
        self.stub = smdbrpc_pb2_grpc.HotshardGatewayStub(channel)

        self.backup_stubs = []
        for i in range(self.num_threads):
            port = self.base_port + i
            destination = "localhost:{}".format(port)
            channel = grpc.insecure_channel(destination)
            self.backup_stubs.append(smdbrpc_pb2_grpc.HotshardGatewayStub(channel))

        # ping the head
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
        promotionResp = self.stub.PromoteKeysToCicada(promotionReq)
        self.assertEqual(
            len(promotionReq.keys), len(
                promotionResp.successfullyPromoted

            )
        )
        for promoted in promotionResp.successfullyPromoted:
            self.assertTrue(promoted)

        time.sleep(2)
        self.now = time.time_ns()

        # ping the head
        key1 = 994813
        key2 = 200604
        key3 = 220604
        response = self.stub.BatchSendTxns(
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
                        value=str(key2).encode(), ), ], ), smdbrpc_pb2.TxnReq(
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
            i += 1

        self.watermark = self.now + 200
        response = self.stub.BatchSendTxns(
            smdbrpc_pb2.BatchSendTxnsReq(
                txns=[smdbrpc_pb2.TxnReq(
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.watermark, logicaltime=0, ),
                    ops=[smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.PUT, table=53, tableName="warehouse",
                        index=1, cicada_key_cols=[key1],
                        key=str(key1).encode(), value=str(key3).encode(), ), ]
                ), smdbrpc_pb2.TxnReq(
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.watermark, logicaltime=0, ),
                    ops=[smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.PUT, table=54, tableName="district",
                        index=1, cicada_key_cols=[key2, key2],
                        key=str(key2).encode(), value=str(key3).encode())]
                ), smdbrpc_pb2.TxnReq(
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.watermark, logicaltime=0, ),
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

    def test_full_pipeline(self):

        # check that the messages are at the backup
        num_pending_txns = 0
        for i in range(self.num_threads):
            req = smdbrpc_pb2.QueryBackupMetaReq()
            resp = self.backup_stubs[i].QueryBackupMeta(req)
            num_pending_txns += resp.num_pending_txns

        self.assertEqual(5, num_pending_txns)

    def test_updated_per_thread_watermark(self):

        # ONLY RUN THIS TEST WITH SINGLE THREADED CICADA

        req = smdbrpc_pb2.QueryThreadMetasReq(
            include_global_watermark=False,
            include_watermarks=True,
            include_logs=False,
        )
        resp = self.stub.QueryThreadMetas(req)
        self.assertEqual(1, len(resp.thread_metas))  # only run with single-threaded Cicada

        self.assertEqual(self.watermark, resp.thread_metas[0].watermark.walltime)

    def test_updated_global_watermark(self):

        # ONLY RUN THIS TEST WITH SINGLE THREADED CICADA

        # check that requests are at backup
        initial_resp = self.backup_stubs[0].QueryBackupMeta(smdbrpc_pb2.QueryBackupMetaReq())
        self.assertEqual(5, initial_resp.num_pending_txns)

        # trigger replay
        _ = self.stub.TriggerReplay(smdbrpc_pb2.TriggerReplayReq())

        # check that no requests exist at backup anymore
        bk_resp = self.backup_stubs[0].QueryBackupMeta(smdbrpc_pb2.QueryBackupMetaReq())
        self.assertEqual(0, bk_resp.num_pending_txns)

        # check that global watermark is the latest timestamp
        wm_req = smdbrpc_pb2.QueryThreadMetasReq(
            include_global_watermark=True,
            include_watermarks=False,
            include_logs=False,
        )
        wm_resp = self.stub.QueryThreadMetas(wm_req)
        self.assertEqual(self.watermark, wm_resp.global_watermark.walltime)

        # send some more requests, see if they get through
        key1 = 994813
        key2 = 200604
        key3 = 220604
        response = self.stub.BatchSendTxns(
            smdbrpc_pb2.BatchSendTxnsReq(
                txns=[smdbrpc_pb2.TxnReq(
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.watermark + 200, logicaltime=0, ),
                    ops=[smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.PUT, table=53, tableName="warehouse",
                        index=1, cicada_key_cols=[key1],
                        key=str(key1).encode(), value=str(key3).encode(), ), ]
                ), smdbrpc_pb2.TxnReq(
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.watermark + 200, logicaltime=0, ),
                    ops=[smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.PUT, table=54, tableName="district",
                        index=1, cicada_key_cols=[key2, key2],
                        key=str(key2).encode(), value=str(key3).encode())]
                ), smdbrpc_pb2.TxnReq(
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.watermark + 200, logicaltime=0, ),
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

        # see if backup got the requests
        second_resp = self.backup_stubs[0].QueryBackupMeta(smdbrpc_pb2.QueryBackupMetaReq())
        self.assertEqual(2, second_resp.num_pending_txns)

        # check that global watermark remains untouched
        second_wm_req = smdbrpc_pb2.QueryThreadMetasReq(
            include_global_watermark=True,
            include_watermarks=False,
            include_logs=False,
        )
        second_wm_resp = self.stub.QueryThreadMetas(second_wm_req)
        self.assertEqual(self.watermark, second_wm_resp.global_watermark.walltime)


if __name__ == '__main__':
    unittest.main()
