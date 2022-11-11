import threading
import unittest

import time
import grpc
import smdbrpc_pb2
import smdbrpc_pb2_grpc


class TestReplicationWithBuffering(unittest.TestCase):

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

    def test_buffering_and_release(self):
        def trigger_replay():
            time.sleep(2)
            _ = self.stub.TriggerReplay(smdbrpc_pb2.TriggerReplayReq())

        t = threading.Thread(target=trigger_replay, args=())
        t.start()

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

        # ... we might have to wait here for the trigger replay to fire
        # ...
        # ...

        self.assertEqual(3, len(response.txnResps))
        for txnResp in response.txnResps:
            self.assertTrue(txnResp.is_committed)