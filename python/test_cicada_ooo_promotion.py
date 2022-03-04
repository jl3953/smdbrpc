import time
import unittest

import grpc

import smdbrpc_pb2
import smdbrpc_pb2_grpc


class TestCicadaOOOPromotion(unittest.TestCase):
    NUM_KEYS_TO_PROMOTE = 10
    GAP = 2
    OOO_CONST = 5

    def setUp(self) -> None:
        self.channel = grpc.insecure_channel("localhost:50051")
        self.stub = smdbrpc_pb2_grpc.HotshardGatewayStub(self.channel)
        self.now = time.time_ns()

    def tearDown(self) -> None:
        self.channel.close()

    def test_promote_in_order_no_gaps(self):

        # promote
        promoReq = smdbrpc_pb2.PromoteKeysToCicadaReq(
            keys=[]
        )
        for i in range(TestCicadaOOOPromotion.NUM_KEYS_TO_PROMOTE):
            promoReq.keys.append(
                smdbrpc_pb2.Key(
                    table=53, index=1, cicada_key_cols=[i], key=str(i).encode(),
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now, logicaltime=0, ),
                    value=str(i).encode(), )
            )
        promoResp = self.stub.PromoteKeysToCicada(promoReq)

        self.assertEqual(
            TestCicadaOOOPromotion.NUM_KEYS_TO_PROMOTE,
            len(promoResp.successfullyPromoted)
        )
        for i in range(TestCicadaOOOPromotion.NUM_KEYS_TO_PROMOTE):
            self.assertTrue(promoResp.successfullyPromoted[i])

        # read promoted keys in backwards order
        txnReq = smdbrpc_pb2.TxnReq(
            ops=[], timestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now + 200, logicaltime=0, )
        )

        for i in range(TestCicadaOOOPromotion.NUM_KEYS_TO_PROMOTE, 0, -1):
            txnReq.ops.append(
                smdbrpc_pb2.Op(
                    cmd=smdbrpc_pb2.GET, table=53, index=1,
                    cicada_key_cols=[i - 1], key=str(i - 1).encode()
                )
            )

        batchSendTxnsReq = smdbrpc_pb2.BatchSendTxnsReq(
            txns=[txnReq]
        )
        batchSendTxnsResp = self.stub.BatchSendTxns(batchSendTxnsReq)
        self.assertEqual(1, len(batchSendTxnsResp.txnResps))
        self.assertTrue(batchSendTxnsResp.txnResps[0].is_committed)
        self.assertEqual(
            TestCicadaOOOPromotion.NUM_KEYS_TO_PROMOTE,
            len(batchSendTxnsResp.txnResps[0].responses)
        )
        for i in range(TestCicadaOOOPromotion.NUM_KEYS_TO_PROMOTE, 0, -1):
            self.assertEqual(
                str(i - 1).encode(), batchSendTxnsResp.txnResps[0].responses[
                    TestCicadaOOOPromotion.NUM_KEYS_TO_PROMOTE - i].value
            )

    def test_promote_in_order_with_gaps(self):

        # promote
        promoReq = smdbrpc_pb2.PromoteKeysToCicadaReq(
            keys=[]
        )
        for i in range(TestCicadaOOOPromotion.NUM_KEYS_TO_PROMOTE):
            promoReq.keys.append(
                smdbrpc_pb2.Key(
                    table=53, index=1,
                    cicada_key_cols=[i * TestCicadaOOOPromotion.GAP],
                    key=str(i * TestCicadaOOOPromotion.GAP).encode(),
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now, logicaltime=0, ),
                    value=str(i * TestCicadaOOOPromotion.GAP).encode(), )
            )
        promoResp = self.stub.PromoteKeysToCicada(promoReq)

        self.assertEqual(
            TestCicadaOOOPromotion.NUM_KEYS_TO_PROMOTE,
            len(promoResp.successfullyPromoted)
        )
        for i in range(TestCicadaOOOPromotion.NUM_KEYS_TO_PROMOTE):
            self.assertTrue(promoResp.successfullyPromoted[i])

        # insert into a gap
        txnReq = smdbrpc_pb2.TxnReq(
            ops=[smdbrpc_pb2.Op(
                cmd=smdbrpc_pb2.PUT, table=53, index=1,
                cicada_key_cols=[TestCicadaOOOPromotion.GAP + 1],
                key=str(TestCicadaOOOPromotion.GAP + 1).encode(),
                value=str(TestCicadaOOOPromotion.GAP + 1).encode()
            )], timestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now + 200, logicaltime=0
            )
        )

        batchSendTxnsReq = smdbrpc_pb2.BatchSendTxnsReq(
            txns=[txnReq]
        )
        batchSendTxnsResp = self.stub.BatchSendTxns(batchSendTxnsReq)
        self.assertEqual(1, len(batchSendTxnsResp.txnResps))
        self.assertTrue(batchSendTxnsResp.txnResps[0].is_committed)

        # read from the gap
        readTxnReq = smdbrpc_pb2.TxnReq(
            ops=[smdbrpc_pb2.Op(
                cmd=smdbrpc_pb2.GET, table=53, index=1,
                cicada_key_cols=[TestCicadaOOOPromotion.GAP + 1],
                key=str(TestCicadaOOOPromotion.GAP + 1).encode(),
                value=str(TestCicadaOOOPromotion.GAP + 1).encode()
            )], timestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now + 300, logicaltime=0
            )
        )
        readBatchTxnsResp = self.stub.BatchSendTxns(
            smdbrpc_pb2.BatchSendTxnsReq(
                txns=[readTxnReq]
            )
        )
        self.assertEqual(1, len(readBatchTxnsResp.txnResps))
        self.assertTrue(readBatchTxnsResp.txnResps[0].is_committed)
        self.assertEqual(1, len(readBatchTxnsResp.txnResps[0].responses))
        self.assertEqual(
            str(TestCicadaOOOPromotion.GAP + 1).encode(),
            readBatchTxnsResp.txnResps[0].responses[0].value
        )

    def test_promote_ooo_no_gaps(self):
        # promote out of order
        promoReq = smdbrpc_pb2.PromoteKeysToCicadaReq(
            keys=[]
        )
        for i in range(
            TestCicadaOOOPromotion.OOO_CONST +
            TestCicadaOOOPromotion.NUM_KEYS_TO_PROMOTE,
            TestCicadaOOOPromotion.OOO_CONST, -1
        ):
            promoReq.keys.append(
                smdbrpc_pb2.Key(
                    table=53, index=1, cicada_key_cols=[
                        i % TestCicadaOOOPromotion.NUM_KEYS_TO_PROMOTE],
                    key=str(
                        i % TestCicadaOOOPromotion.NUM_KEYS_TO_PROMOTE
                    ).encode(), timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now, logicaltime=0
                    ), value=str(
                        i % TestCicadaOOOPromotion.NUM_KEYS_TO_PROMOTE
                    ).encode()
                )
            )
        promoResp = self.stub.PromoteKeysToCicada(promoReq)

        self.assertEqual(
            TestCicadaOOOPromotion.NUM_KEYS_TO_PROMOTE,
            len(promoResp.successfullyPromoted)
        )
        for i in range(TestCicadaOOOPromotion.NUM_KEYS_TO_PROMOTE):
            self.assertTrue(promoResp.successfullyPromoted[i])

        # read in order
        txnReq = smdbrpc_pb2.TxnReq(
            ops=[], timestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now + 200, logicaltime=0, )
        )

        for i in range(TestCicadaOOOPromotion.NUM_KEYS_TO_PROMOTE):
            txnReq.ops.append(
                smdbrpc_pb2.Op(
                    cmd=smdbrpc_pb2.GET, table=53, index=1, cicada_key_cols=[i],
                    key=str(i).encode()
                )
            )

        batchSendTxnsReq = smdbrpc_pb2.BatchSendTxnsReq(
            txns=[txnReq]
        )
        batchSendTxnsResp = self.stub.BatchSendTxns(batchSendTxnsReq)
        self.assertEqual(1, len(batchSendTxnsResp.txnResps))
        self.assertTrue(batchSendTxnsResp.txnResps[0].is_committed)
        self.assertEqual(
            TestCicadaOOOPromotion.NUM_KEYS_TO_PROMOTE,
            len(batchSendTxnsResp.txnResps[0].responses)
        )
        for i in range(TestCicadaOOOPromotion.NUM_KEYS_TO_PROMOTE):
            self.assertEqual(
                str(i).encode(),
                batchSendTxnsResp.txnResps[0].responses[i].value
            )

    def test_promote_ooo_with_gaps(self):
        # promote out of order with gaps
        promoReq = smdbrpc_pb2.PromoteKeysToCicadaReq(
            keys=[]
        )
        for i in range(
            TestCicadaOOOPromotion.OOO_CONST +
            TestCicadaOOOPromotion.NUM_KEYS_TO_PROMOTE,
            TestCicadaOOOPromotion.OOO_CONST, -1
        ):
            promoReq.keys.append(
                smdbrpc_pb2.Key(
                    table=53, index=1, cicada_key_cols=[
                        i % TestCicadaOOOPromotion.NUM_KEYS_TO_PROMOTE *
                        TestCicadaOOOPromotion.GAP],
                    key=str(
                        i % TestCicadaOOOPromotion.NUM_KEYS_TO_PROMOTE *
                        TestCicadaOOOPromotion.GAP
                    ).encode(), timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now, logicaltime=0
                    ), value=str(
                        i % TestCicadaOOOPromotion.NUM_KEYS_TO_PROMOTE *
                        TestCicadaOOOPromotion.GAP
                    ).encode()
                )
            )
        promoResp = self.stub.PromoteKeysToCicada(promoReq)

        self.assertEqual(
            TestCicadaOOOPromotion.NUM_KEYS_TO_PROMOTE,
            len(promoResp.successfullyPromoted)
        )
        for i in range(TestCicadaOOOPromotion.NUM_KEYS_TO_PROMOTE):
            self.assertTrue(promoResp.successfullyPromoted[i])

        # insert into a gap
        txnReq = smdbrpc_pb2.TxnReq(
            ops=[smdbrpc_pb2.Op(
                cmd=smdbrpc_pb2.PUT, table=53, index=1,
                cicada_key_cols=[TestCicadaOOOPromotion.GAP + 1],
                key=str(TestCicadaOOOPromotion.GAP + 1).encode(),
                value=str(TestCicadaOOOPromotion.GAP + 1).encode()
            )], timestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now + 200, logicaltime=0
            )
        )

        batchSendTxnsReq = smdbrpc_pb2.BatchSendTxnsReq(
            txns=[txnReq]
        )
        batchSendTxnsResp = self.stub.BatchSendTxns(batchSendTxnsReq)
        self.assertEqual(1, len(batchSendTxnsResp.txnResps))
        self.assertTrue(batchSendTxnsResp.txnResps[0].is_committed)

        # read from the gap
        readTxnReq = smdbrpc_pb2.TxnReq(
            ops=[smdbrpc_pb2.Op(
                cmd=smdbrpc_pb2.GET, table=53, index=1,
                cicada_key_cols=[TestCicadaOOOPromotion.GAP + 1],
                key=str(TestCicadaOOOPromotion.GAP + 1).encode(),
                value=str(TestCicadaOOOPromotion.GAP + 1).encode()
            )], timestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now + 300, logicaltime=0
            )
        )
        readBatchTxnsResp = self.stub.BatchSendTxns(
            smdbrpc_pb2.BatchSendTxnsReq(
                txns=[readTxnReq]
            )
        )
        self.assertEqual(1, len(readBatchTxnsResp.txnResps))
        self.assertTrue(readBatchTxnsResp.txnResps[0].is_committed)
        self.assertEqual(1, len(readBatchTxnsResp.txnResps[0].responses))
        self.assertEqual(
            str(TestCicadaOOOPromotion.GAP + 1).encode(),
            readBatchTxnsResp.txnResps[0].responses[0].value
        )

    if __name__ == "__main__":
        unittest.main()
