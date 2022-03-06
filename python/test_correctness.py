import time

import grpc

import smdbrpc_pb2
import smdbrpc_pb2_grpc

import unittest


class TestCicadaSingleKeyTxns(unittest.TestCase):

    def setUp(self):
        self.channel = grpc.insecure_channel("localhost:50051")
        self.stub = smdbrpc_pb2_grpc.HotshardGatewayStub(self.channel)
        self.now = time.time_ns()

    def tearDown(self) -> None:
        self.channel.close()

    # def test_simple_read(self):
    #   """
    #   Tests that reading an un-inserted value comes back empty.
    #   """
    #   key = 1994214
    #   op = smdbrpc_pb2.Op(
    #     cmd=smdbrpc_pb2.GET,
    #     table=53,
    #     index=1,
    #     cicada_key_cols=[key],
    #     key=key.to_bytes(64, "big"),
    #   )
    #   timestamp = smdbrpc_pb2.HLCTimestamp(
    #     walltime=self.now + 100,
    #     logicaltime=0,
    #   )
    #   txnReq = smdbrpc_pb2.TxnReq(
    #     ops=[op],
    #     timestamp=timestamp,
    #   )
    #   response = self.stub.BatchSendTxns(
    #     smdbrpc_pb2.BatchSendTxnsReq(
    #       txns=[txnReq],
    #     )
    #   )
    #   self.assertEqual(1, len(response.txnResps))
    #   self.assertTrue(response.txnResps[0].is_committed)
    #   self.assertEqual(0, len(response.txnResps[0].responses))

    def test_demote_write(self):
        key = 994812
        val = "DEMOTION_VALUE".encode()
        op = smdbrpc_pb2.Op(
            cmd=smdbrpc_pb2.PUT, table=53, index=1, cicada_key_cols=[key],
            key="hello".encode(), value=val, )
        timestamp = smdbrpc_pb2.HLCTimestamp(
            walltime=self.now, logicaltime=0, )
        _ = self.stub.BatchSendTxns(
            smdbrpc_pb2.BatchSendTxnsReq(
                txns=[smdbrpc_pb2.TxnReq(
                    ops=[op], timestamp=timestamp, is_test=False,
                    is_demoted_test_field=True, )]
            )
        )

    def test_write(self):
        key = 994812
        val = "testing".encode()
        op = smdbrpc_pb2.Op(
            cmd=smdbrpc_pb2.PUT, table=53, index=1, cicada_key_cols=[key],
            key="hello".encode(), value=val, )
        timestamp = smdbrpc_pb2.HLCTimestamp(
            walltime=self.now + 10000, logicaltime=0, )
        _ = self.stub.BatchSendTxns(
            smdbrpc_pb2.BatchSendTxnsReq(
                txns=[smdbrpc_pb2.TxnReq(
                    ops=[op], timestamp=timestamp, is_test=True, )]
            )
        )

    def test_simple_write(self):
        key = 994215
        val = "hello".encode()
        op = smdbrpc_pb2.Op(
            cmd=smdbrpc_pb2.PUT, table=53, index=1, cicada_key_cols=[key],
            key="hello".encode(), value=val, )
        timestamp = smdbrpc_pb2.HLCTimestamp(
            walltime=self.now + 1000, logicaltime=0, )
        promoResp = self.stub.PromoteKeysToCicada(
            smdbrpc_pb2.PromoteKeysToCicadaReq(
                keys=[smdbrpc_pb2.Key(
                    table=53, index=1, cicada_key_cols=[key], key="hello".encode(),
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now, logicaltime=0, ), value=val, )]
            )
        )
        self.assertEqual(1, len(promoResp.successfullyPromoted))
        self.assertTrue(promoResp.successfullyPromoted[0])
        response = self.stub.BatchSendTxns(
            smdbrpc_pb2.BatchSendTxnsReq(
                txns=[smdbrpc_pb2.TxnReq(
                    ops=[op], timestamp=timestamp, )]
            )
        )
        self.assertEqual(1, len(response.txnResps))
        self.assertTrue(response.txnResps[0].is_committed)

        op = smdbrpc_pb2.Op(
            cmd=smdbrpc_pb2.GET, table=53, index=1, cicada_key_cols=[key], key=val, )
        response = self.stub.BatchSendTxns(
            smdbrpc_pb2.BatchSendTxnsReq(
                txns=[smdbrpc_pb2.TxnReq(
                    ops=[op], timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now + 200, logicaltime=0, ), )]
            )
        )
        self.assertEqual(1, len(response.txnResps))
        self.assertTrue(response.txnResps[0].is_committed)
        self.assertEqual(1, len(response.txnResps[0].responses))
        self.assertEqual(val, response.txnResps[0].responses[0].value)

        op = smdbrpc_pb2.Op(
            cmd=smdbrpc_pb2.GET, table=53, index=1, cicada_key_cols=[key], key=val, )
        response = self.stub.BatchSendTxns(
            smdbrpc_pb2.BatchSendTxnsReq(
                txns=[smdbrpc_pb2.TxnReq(
                    ops=[op], timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now + 200, logicaltime=0, ), )]
            )
        )
        self.assertEqual(1, len(response.txnResps))
        self.assertTrue(response.txnResps[0].is_committed)
        self.assertEqual(1, len(response.txnResps[0].responses))
        self.assertEqual(val, response.txnResps[0].responses[0].value)

    def test_fail_on_write_under_read(self):
        key = 994216
        promoResp = self.stub.PromoteKeysToCicada(
            smdbrpc_pb2.PromoteKeysToCicadaReq(
                keys=[smdbrpc_pb2.Key(
                    table=53, index=1, cicada_key_cols=[key], key="promoted".encode(),
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now, logicaltime=0, ),
                    value="promoted".encode(), )]
            )
        )
        self.assertEqual(1, len(promoResp.successfullyPromoted))
        self.assertTrue(promoResp.successfullyPromoted[0])
        response = self.stub.BatchSendTxns(
            smdbrpc_pb2.BatchSendTxnsReq(
                txns=[smdbrpc_pb2.TxnReq(
                    ops=[smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.PUT, table=53, index=1, cicada_key_cols=[key],
                        key=str(key).encode(), value=str(key).encode(), )],
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now + 100, logicaltime=0, )
                )]
            )
        )
        self.assertEqual(1, len(response.txnResps))
        self.assertTrue(response.txnResps[0].is_committed)
        response = self.stub.BatchSendTxns(
            smdbrpc_pb2.BatchSendTxnsReq(
                txns=[smdbrpc_pb2.TxnReq(
                    ops=[smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.GET, table=53, index=1, cicada_key_cols=[key],
                        key=str(key).encode(), )],
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now + 200, logicaltime=0, ), )]
            )
        )
        self.assertEqual(1, len(response.txnResps))
        self.assertTrue(response.txnResps[0].is_committed)
        self.assertEqual(1, len(response.txnResps[0].responses))
        self.assertEqual(
            str(key).encode(), response.txnResps[0].responses[0].value
        )
        response = self.stub.BatchSendTxns(
            smdbrpc_pb2.BatchSendTxnsReq(
                txns=[smdbrpc_pb2.TxnReq(
                    ops=[smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.PUT, table=53, index=1, cicada_key_cols=[key],
                        key=str(key).encode(), value=str(key + 1).encode(), )],
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now + 150, logicaltime=0, ), )]
            )
        )
        self.assertEqual(1, len(response.txnResps))
        self.assertFalse(response.txnResps[0].is_committed)
        response = self.stub.BatchSendTxns(
            smdbrpc_pb2.BatchSendTxnsReq(
                txns=[smdbrpc_pb2.TxnReq(
                    ops=[smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.GET, table=53, index=1, cicada_key_cols=[key],
                        key=str(key).encode(), )],
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now + 300, logicaltime=0, )
                )]
            )
        )
        self.assertEqual(1, len(response.txnResps))
        self.assertTrue(response.txnResps[0].is_committed)
        self.assertTrue(1, len(response.txnResps[0].responses))
        self.assertEqual(
            str(key).encode(), response.txnResps[0].responses[0].value
        )

    def test_succeed_on_non_recent_read(self):
        key = 994217
        promoResp = self.stub.PromoteKeysToCicada(
            smdbrpc_pb2.PromoteKeysToCicadaReq(
                keys=[smdbrpc_pb2.Key(
                    table=53, index=1, cicada_key_cols=[key], key="promoted".encode(),
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now, logicaltime=0, ),
                    value="promoted".encode(), )]
            )
        )
        self.assertEqual(1, len(promoResp.successfullyPromoted))
        self.assertTrue(promoResp.successfullyPromoted[0])
        response = self.stub.BatchSendTxns(
            smdbrpc_pb2.BatchSendTxnsReq(
                txns=[smdbrpc_pb2.TxnReq(
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now + 100, logicaltime=0, ),
                    ops=[smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.PUT, table=53, index=1, cicada_key_cols=[key],
                        key=str(key).encode(), value=str(key).encode(), )], )]
            )
        )
        self.assertEqual(1, len(response.txnResps))
        self.assertTrue(response.txnResps[0].is_committed)
        response = self.stub.BatchSendTxns(
            smdbrpc_pb2.BatchSendTxnsReq(
                txns=[smdbrpc_pb2.TxnReq(
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now + 200, logicaltime=0, ),
                    ops=[smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.PUT, table=53, index=1, cicada_key_cols=[key],
                        key=str(key).encode(),
                        value=str(key + 1).encode(), )], )]
            )
        )
        self.assertEqual(1, len(response.txnResps))
        self.assertTrue(response.txnResps[0].is_committed)
        response = self.stub.BatchSendTxns(
            smdbrpc_pb2.BatchSendTxnsReq(
                txns=[smdbrpc_pb2.TxnReq(
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now + 150, logicaltime=0, ),
                    ops=[smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.GET, table=53, index=1, cicada_key_cols=[key],
                        key=str(key).encode(), )], )]
            )
        )
        self.assertEqual(1, len(response.txnResps))
        self.assertTrue(response.txnResps[0].is_committed)
        self.assertTrue(1, len(response.txnResps[0].responses))
        self.assertEqual(
            str(key).encode(), response.txnResps[0].responses[0].value
        )

    def test_fail_on_write_under_write(self):
        key = 994218
        promoResp = self.stub.PromoteKeysToCicada(
            smdbrpc_pb2.PromoteKeysToCicadaReq(
                keys=[smdbrpc_pb2.Key(
                    table=53, index=1, cicada_key_cols=[key], key="promoted".encode(),
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now, logicaltime=0, ),
                    value="promoted".encode(), )]
            )
        )
        self.assertEqual(1, len(promoResp.successfullyPromoted))
        self.assertTrue(promoResp.successfullyPromoted[0])
        response = self.stub.BatchSendTxns(
            smdbrpc_pb2.BatchSendTxnsReq(
                txns=[smdbrpc_pb2.TxnReq(
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now + 100, logicaltime=0, ),
                    ops=[smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.PUT, table=53, index=1, cicada_key_cols=[key],
                        key=str(key).encode(), value=str(key).encode(), ), ], )]
            )
        )
        self.assertEqual(1, len(response.txnResps))
        self.assertTrue(response.txnResps[0].is_committed)
        response = self.stub.BatchSendTxns(
            smdbrpc_pb2.BatchSendTxnsReq(
                txns=[smdbrpc_pb2.TxnReq(
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now + 200, logicaltime=0, ),
                    ops=[smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.PUT, table=53, index=1, cicada_key_cols=[key],
                        key=str(key).encode(),
                        value=str(key + 1).encode(), )], )]
            )
        )
        self.assertEqual(1, len(response.txnResps))
        self.assertTrue(response.txnResps[0].is_committed)
        response = self.stub.BatchSendTxns(
            smdbrpc_pb2.BatchSendTxnsReq(
                txns=[smdbrpc_pb2.TxnReq(
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now + 150, logicaltime=0, ),
                    ops=[smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.PUT, table=53, index=1, cicada_key_cols=[key],
                        key=str(key).encode(),
                        value=str(key + 10).encode(), )], )]
            )
        )
        self.assertEqual(1, len(response.txnResps))
        self.assertFalse(response.txnResps[0].is_committed)
        response = self.stub.BatchSendTxns(
            smdbrpc_pb2.BatchSendTxnsReq(
                txns=[smdbrpc_pb2.TxnReq(
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now + 300, logicaltime=0, ),
                    ops=[smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.GET, table=53, index=1, cicada_key_cols=[key],
                        key=str(key).encode(), )], )]
            )
        )
        self.assertEqual(1, len(response.txnResps))
        self.assertTrue(response.txnResps[0].is_committed)
        self.assertEqual(1, len(response.txnResps[0].responses))
        self.assertEqual(
            str(key + 1).encode(), response.txnResps[0].responses[0].value
        )


class TestCicadaMultiKeyTxns(unittest.TestCase):

    def setUp(self):
        self.channel = grpc.insecure_channel("localhost:50051")
        self.stub = smdbrpc_pb2_grpc.HotshardGatewayStub(self.channel)
        self.now = time.time_ns()

        promotionReq = smdbrpc_pb2.PromoteKeysToCicadaReq(
            keys=[smdbrpc_pb2.Key(
                table=53, index=1, cicada_key_cols=[994813], key="promoted".encode(),
                timestamp=smdbrpc_pb2.HLCTimestamp(
                    walltime=self.now, logicaltime=0, ),
                value="promoted".encode(), ), smdbrpc_pb2.Key(
                table=53, index=1, cicada_key_cols=[200604], key="promoted".encode(),
                timestamp=smdbrpc_pb2.HLCTimestamp(
                    walltime=self.now, logicaltime=0, ),
                value="promoted".encode(), ), smdbrpc_pb2.Key(
                table=53, index=1, cicada_key_cols=[994814], key="promoted".encode(),
                timestamp=smdbrpc_pb2.HLCTimestamp(
                    walltime=self.now, logicaltime=0, ),
                value="promoted".encode(), ), smdbrpc_pb2.Key(
                table=53, index=1, cicada_key_cols=[200605], key="promoted".encode(),
                timestamp=smdbrpc_pb2.HLCTimestamp(
                    walltime=self.now, logicaltime=0, ),
                value="promoted".encode(), ), smdbrpc_pb2.Key(
                table=53, index=1, cicada_key_cols=[994815], key="promoted".encode(),
                timestamp=smdbrpc_pb2.HLCTimestamp(
                    walltime=self.now, logicaltime=0, ),
                value="promoted".encode(), ), smdbrpc_pb2.Key(
                table=53, index=1, cicada_key_cols=[200606], key="promoted".encode(),
                timestamp=smdbrpc_pb2.HLCTimestamp(
                    walltime=self.now, logicaltime=0, ),
                value="promoted".encode(), ), smdbrpc_pb2.Key(
                table=53, index=1, cicada_key_cols=[994816], key="promoted".encode(),
                timestamp=smdbrpc_pb2.HLCTimestamp(
                    walltime=self.now, logicaltime=0, ),
                value="promoted".encode(), ), smdbrpc_pb2.Key(
                table=53, index=1, cicada_key_cols=[200607], key="promoted".encode(),
                timestamp=smdbrpc_pb2.HLCTimestamp(
                    walltime=self.now, logicaltime=0, ),
                value="promoted".encode(), ), ]
        )

        promotionResp = self.stub.PromoteKeysToCicada(promotionReq)
        self.assertEqual(
            len(promotionReq.keys), len(promotionResp.successfullyPromoted)
        )
        for promoted in promotionResp.successfullyPromoted:
            self.assertTrue(promoted)

    def tearDown(self) -> None:
        self.channel.close()

    # def test_simple_read(self):
    #   key1 = 1994812
    #   key2 = 20200603
    #   response = self.stub.SendTxn(smdbrpc_pb2.TxnReq(
    #     timestamp=smdbrpc_pb2.HLCTimestamp(
    #       walltime=self.now + 100,
    #       logicaltime=0,
    #     ),
    #     ops=[
    #       smdbrpc_pb2.Op(
    #         cmd=smdbrpc_pb2.GET,
    #         table=53,
    #         index=1,
    #         cicada_key_cols=[key1],
    #         key=str(key1).encode(),
    #       ),
    #       smdbrpc_pb2.Op(
    #         cmd=smdbrpc_pb2.GET,
    #         table=53,
    #         index=1,
    #         cicada_key_cols=[key2],
    #         key=str(key2).encode(),
    #       )
    #     ]
    #   ))
    #   self.assertTrue(response.is_committed)
    #   self.assertEqual(0, len(response.responses))

    def test_simple_write(self):
        key1 = 994813
        key2 = 200604
        response = self.stub.BatchSendTxns(
            smdbrpc_pb2.BatchSendTxnsReq(
                txns=[smdbrpc_pb2.TxnReq(
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now + 100, logicaltime=0, ),
                    ops=[smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.PUT, table=53, index=1, cicada_key_cols=[key1],
                        key=str(key1).encode(),
                        value=str(key1).encode(), ), ], ), smdbrpc_pb2.TxnReq(
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now + 100, logicaltime=0, ),
                    ops=[smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.PUT, table=53, index=1, cicada_key_cols=[key2],
                        key=str(key2).encode(),
                        value=str(key2).encode(), ), ], )]
            )
        )
        self.assertEqual(2, len(response.txnResps))
        for txnResp in response.txnResps:
            self.assertTrue(txnResp.is_committed)

        response = self.stub.BatchSendTxns(
            smdbrpc_pb2.BatchSendTxnsReq(
                txns=[smdbrpc_pb2.TxnReq(
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now + 200, logicaltime=0, ),
                    ops=[smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.GET, table=53, index=1, cicada_key_cols=[key1],
                        key=str(key1).encode(), ), ]
                ), smdbrpc_pb2.TxnReq(
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now + 200, logicaltime=0, ),
                    ops=[smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.GET, table=53, index=1, cicada_key_cols=[key2],
                        key=str(key2).encode(), )]
                )]
            )
        )
        self.assertEqual(2, len(response.txnResps))
        for txnResp in response.txnResps:
            self.assertTrue(txnResp.is_committed)

        self.assertEqual(
            str(key1).encode(), response.txnResps[0].responses[0].value
        )
        self.assertEqual(
            str(key2).encode(), response.txnResps[1].responses[0].value
        )

    def test_succeed_on_non_recent_read(self):
        key1 = 994814
        key2 = 200605
        response = self.stub.BatchSendTxns(
            smdbrpc_pb2.BatchSendTxnsReq(
                txns=[smdbrpc_pb2.TxnReq(
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now + 100, logicaltime=0, ),
                    ops=[smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.PUT, table=53, index=1, cicada_key_cols=[key1],
                        key=str(key1).encode(), value=str(key1).encode(), ), ]
                ), smdbrpc_pb2.TxnReq(
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now + 100, logicaltime=0, ),
                    ops=[smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.PUT, table=53, index=1, cicada_key_cols=[key2],
                        key=str(key2).encode(), value=str(key2).encode(), )]
                )]
            )
        )
        self.assertEqual(2, len(response.txnResps))
        self.assertTrue(response.txnResps[0].is_committed)
        self.assertTrue(response.txnResps[1].is_committed)

        response = self.stub.BatchSendTxns(
            smdbrpc_pb2.BatchSendTxnsReq(
                txns=[smdbrpc_pb2.TxnReq(
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now + 200, logicaltime=0, ),
                    ops=[smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.PUT, table=53, index=1, cicada_key_cols=[key1],
                        key=str(key1).encode(), value=str(key1 + 1).encode(), )]
                ), smdbrpc_pb2.TxnReq(
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now + 200, logicaltime=0, ),
                    ops=[smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.PUT, table=53, index=1, cicada_key_cols=[key1],
                        key=str(key1).encode(), value=str(key1 + 1).encode(), )]
                )]
            )
        )
        self.assertEqual(2, len(response.txnResps))
        self.assertTrue(response.txnResps[0].is_committed)
        self.assertTrue(response.txnResps[1].is_committed)
        response = self.stub.BatchSendTxns(
            smdbrpc_pb2.BatchSendTxnsReq(
                txns=[smdbrpc_pb2.TxnReq(
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now + 150, logicaltime=0, ),
                    ops=[smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.GET, table=53, index=1, cicada_key_cols=[key1],
                        key=str(key1).encode(), ), smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.GET, table=53, index=1, cicada_key_cols=[key2],
                        key=str(key2).encode(), ), ]
                )]
            )
        )
        self.assertEqual(1, len(response.txnResps))
        self.assertTrue(response.txnResps[0].is_committed)
        self.assertEqual(2, len(response.txnResps[0].responses))
        self.assertEqual(
            str(key1).encode(), response.txnResps[0].responses[0].value
        )
        self.assertEqual(
            str(key2).encode(), response.txnResps[0].responses[1].value
        )

    def test_succeed_on_disjoint_updates(self):
        key1 = 994815
        key2 = 200606
        response = self.stub.BatchSendTxns(
            smdbrpc_pb2.BatchSendTxnsReq(
                txns=[smdbrpc_pb2.TxnReq(
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now + 100, logicaltime=0, ),
                    ops=[smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.PUT, table=53, index=1, cicada_key_cols=[key1],
                        key=str(key1).encode(), value=str(key1).encode(), ),
                        smdbrpc_pb2.Op(
                            cmd=smdbrpc_pb2.PUT, table=53, index=1,
                            cicada_key_cols=[key2], key=str(key2).encode(),
                            value=str(key2).encode(), )]
                )]
            )
        )
        self.assertEqual(1, len(response.txnResps))
        self.assertTrue(response.txnResps[0].is_committed)

        response = self.stub.BatchSendTxns(
            smdbrpc_pb2.BatchSendTxnsReq(
                txns=[smdbrpc_pb2.TxnReq(
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now + 200, logicaltime=0, ),
                    ops=[smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.PUT, table=53, index=1, cicada_key_cols=[key1],
                        key=str(key1).encode(), value=str(key1 + 1).encode()
                    ), ]
                )]
            )
        )
        self.assertEqual(1, len(response.txnResps))
        self.assertTrue(response.txnResps[0].is_committed)

        response = self.stub.BatchSendTxns(
            smdbrpc_pb2.BatchSendTxnsReq(
                txns=[smdbrpc_pb2.TxnReq(
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now + 300, logicaltime=0, ),
                    ops=[smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.GET, table=53, index=1, cicada_key_cols=[key1],
                        key=str(key1).encode(), ), smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.GET, table=53, index=1, cicada_key_cols=[key2],
                        key=str(key2).encode(), ), ], )]
            )
        )
        self.assertEqual(1, len(response.txnResps))
        self.assertTrue(response.txnResps[0].is_committed)
        self.assertEqual(2, len(response.txnResps[0].responses))
        self.assertEqual(
            str(key1 + 1).encode(), response.txnResps[0].responses[0].value
        )
        self.assertEqual(
            str(key2).encode(), response.txnResps[0].responses[1].value
        )

    def test_fail_on_write_between_disjoint_updates(self):
        key1 = 994816
        key2 = 200607
        response = self.stub.BatchSendTxns(
            smdbrpc_pb2.BatchSendTxnsReq(
                txns=[smdbrpc_pb2.TxnReq(
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now + 100, logicaltime=0, ),
                    ops=[smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.PUT, table=53, index=1, cicada_key_cols=[key1],
                        key=str(key1).encode(), value=str(key1).encode(), ),
                        smdbrpc_pb2.Op(
                            cmd=smdbrpc_pb2.PUT, table=53, index=1,
                            cicada_key_cols=[key2], key=str(key2).encode(),
                            value=str(key2).encode(), )]
                )]
            )
        )
        self.assertEqual(1, len(response.txnResps))
        self.assertTrue(response.txnResps[0].is_committed)

        response = self.stub.BatchSendTxns(
            smdbrpc_pb2.BatchSendTxnsReq(
                txns=[smdbrpc_pb2.TxnReq(
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now + 200, logicaltime=0, ),
                    ops=[smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.PUT, table=53, index=1, cicada_key_cols=[key1],
                        key=str(key1).encode(), value=str(key1 + 1).encode(), )]
                )]
            )
        )
        self.assertEqual(1, len(response.txnResps))
        self.assertTrue(response.txnResps[0].is_committed)
        response = self.stub.BatchSendTxns(
            smdbrpc_pb2.BatchSendTxnsReq(
                txns=[smdbrpc_pb2.TxnReq(
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now + 150, logicaltime=0, ),
                    ops=[smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.PUT, table=53, index=1, cicada_key_cols=[key1],
                        key=str(key1).encode(), value=str(key1 + 10).encode(),

                    ), smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.PUT, table=53, index=1, cicada_key_cols=[key2],
                        key=str(key2).encode(),
                        value=str(key2 + 10).encode(), )]
                )]
            )
        )
        self.assertEqual(1, len(response.txnResps))
        self.assertFalse(response.txnResps[0].is_committed)
        response = self.stub.BatchSendTxns(
            smdbrpc_pb2.BatchSendTxnsReq(
                txns=[smdbrpc_pb2.TxnReq(
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now + 300, logicaltime=0, ),
                    ops=[smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.GET, table=53, index=1, cicada_key_cols=[key1],
                        key=str(key1).encode(), ), smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.GET, table=53, index=1, cicada_key_cols=[key2],
                        key=str(key2).encode(), )]
                )]
            )
        )
        self.assertEqual(1, len(response.txnResps))
        self.assertTrue(response.txnResps[0].is_committed)
        self.assertEqual(2, len(response.txnResps[0].responses))
        self.assertEqual(
            str(key1 + 1).encode(), response.txnResps[0].responses[0].value
        )
        self.assertEqual(
            str(key2).encode(), response.txnResps[0].responses[1].value
        )


if __name__ == '__main__':
    unittest.main()
