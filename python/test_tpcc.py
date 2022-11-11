import threading
import unittest
import smdbrpc_pb2
import grpc
import time
import smdbrpc_pb2_grpc


class TestCicadaMultiKeyTxns(unittest.TestCase):

    def setUp(self):
        self.channel = grpc.insecure_channel("localhost:50051")
        print("stub...")
        self.stub = smdbrpc_pb2_grpc.HotshardGatewayStub(self.channel)
        print("now...")
        self.now = time.time_ns() + 500000000

        print("promoting...")
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
                table=55, tableName="customer", index=1,
                cicada_key_cols=[994814, 994814, 994814],
                key="promoted".encode(), timestamp=smdbrpc_pb2.HLCTimestamp(
                    walltime=self.now, logicaltime=0, ),
                value="promoted".encode(), ), smdbrpc_pb2.Key(
                table=56, tableName="history", index=1,
                cicada_key_cols=[200605], key="promoted".encode(),
                timestamp=smdbrpc_pb2.HLCTimestamp(
                    walltime=self.now, logicaltime=0, ),
                value="promoted".encode(), ), smdbrpc_pb2.Key(
                table=57, tableName="neworder", index=1,
                cicada_key_cols=[994815, 994815, 995815],
                key="promoted".encode(), timestamp=smdbrpc_pb2.HLCTimestamp(
                    walltime=self.now, logicaltime=0, ),
                value="promoted".encode(), ), smdbrpc_pb2.Key(
                table=58, tableName="order", index=1,
                cicada_key_cols=[200606, 200606, 200606],
                key="promoted".encode(), timestamp=smdbrpc_pb2.HLCTimestamp(
                    walltime=self.now, logicaltime=0, ),
                value="promoted".encode(), ), smdbrpc_pb2.Key(
                table=59, tableName="orderline", index=1,
                cicada_key_cols=[994816, 994816, 994816, 994816],
                key="promoted".encode(), timestamp=smdbrpc_pb2.HLCTimestamp(
                    walltime=self.now, logicaltime=0, ),
                value="promoted".encode(), ), smdbrpc_pb2.Key(
                table=60, tableName="item", index=1, cicada_key_cols=[200607],
                key="promoted".encode(), timestamp=smdbrpc_pb2.HLCTimestamp(
                    walltime=self.now, logicaltime=0, ),
                value="promoted".encode(), ), smdbrpc_pb2.Key(
                table=61, tableName="stock", index=1,
                cicada_key_cols=[200608, 200608], key="promoted".encode(),
                timestamp=smdbrpc_pb2.HLCTimestamp(
                    walltime=self.now, logicaltime=0, ), ), ]
        )
        print("sending promotion...")
        promotionResp = self.stub.PromoteKeysToCicada(promotionReq)
        print("asserting response...")
        self.assertEqual(
            len(promotionReq.keys), len(
                promotionResp.successfullyPromoted

            )
        )
        for promoted in promotionResp.successfullyPromoted:
            self.assertTrue(promoted)

        time.sleep(2)
        self.now = time.time_ns()
        print("finished setup...")

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
    #         table=53, tableName="warehouse",
    #         index=1,
    #         cicada_key_cols=[key1],
    #         key=str(key1).encode(),
    #       ),
    #       smdbrpc_pb2.Op(
    #         cmd=smdbrpc_pb2.GET,
    #         table=53, tableName="warehouse",
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
        key3 = 220604
        print("starting test...")
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
                        index=1, cicada_key_cols=[key2, key2], key=str(
                            key2
                        ).encode(), value=str(key2).encode(), ), ], ),
                    smdbrpc_pb2.TxnReq(
                        timestamp=smdbrpc_pb2.HLCTimestamp(
                            walltime=self.now + 100, logicaltime=0, ),
                        ops=[smdbrpc_pb2.Op(
                            cmd=smdbrpc_pb2.PUT, table=61, tableName="stock",
                            index=1, cicada_key_cols=[key3, key3], key=str(
                                key3
                            ).encode(), value=str(key3).encode(), ), ], ), ]
            )
        )
        print("sendBatchTxn...")
        self.assertEqual(3, len(response.txnResps))
        for txnResp in response.txnResps:
            self.assertTrue(txnResp.is_committed)

        def following_put():
            time.sleep(2)
            print(self.now + 300)
            response2 = self.stub.BatchSendTxns(
                smdbrpc_pb2.BatchSendTxnsReq(
                    txns=[smdbrpc_pb2.TxnReq(
                        timestamp=smdbrpc_pb2.HLCTimestamp(
                            walltime=self.now + 300, logicaltime=0, ),
                        ops=[smdbrpc_pb2.Op(
                            cmd=smdbrpc_pb2.PUT, table=53, tableName="warehouse",
                            index=1, cicada_key_cols=[key1],
                            key=str(key1).encode(), ), ]
                    )]
                )
            )

            self.assertEqual(1, len(response2.txnResps))
            for txnResp in response2.txnResps:
                self.assertTrue(txnResp.is_committed)

        t = threading.Thread(target=following_put, args=())
        t.start()

        response = self.stub.BatchSendTxns(
            smdbrpc_pb2.BatchSendTxnsReq(
                txns=[smdbrpc_pb2.TxnReq(
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now + 200, logicaltime=0, ),
                    ops=[smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.GET, table=53, tableName="warehouse",
                        index=1, cicada_key_cols=[key1],
                        key=str(key1).encode(), ), ]
                ), smdbrpc_pb2.TxnReq(
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now + 200, logicaltime=0, ),
                    ops=[smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.GET, table=54, tableName="district",
                        index=1, cicada_key_cols=[key2, key2],
                        key=str(key2).encode(), )]
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

        self.assertEqual(
            str(key1).encode(), response.txnResps[0].responses[0].value
        )
        self.assertEqual(
            str(key2).encode(), response.txnResps[1].responses[0].value
        )
        self.assertEqual(
            str(key3).encode(), response.txnResps[2].responses[0].value
        )

        print("jenndebug ===== hello?")

        response3 = self.stub.BatchSendTxns(
            smdbrpc_pb2.BatchSendTxnsReq(
                txns=[smdbrpc_pb2.TxnReq(
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now + 400, logicaltime=0, ),
                    ops=[smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.PUT, table=53, tableName="warehouse",
                        index=1, cicada_key_cols=[key1],
                        key=str(key1).encode(), ), ]
                )]
            )
        )

        self.assertEqual(1, len(response3.txnResps))
        for txnResp in response3.txnResps:
            self.assertTrue(txnResp.is_committed)

    def test_succeed_on_non_recent_read(self):
        key1 = 994814
        key2 = 200605
        response = self.stub.BatchSendTxns(
            smdbrpc_pb2.BatchSendTxnsReq(
                txns=[smdbrpc_pb2.TxnReq(
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now + 100, logicaltime=0, ),
                    ops=[smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.PUT, table=55, tableName="customer",
                        index=1, cicada_key_cols=[key1, key1, key1], key=str(
                            key1
                        ).encode(), value=str(key1).encode(), ), ]
                ), smdbrpc_pb2.TxnReq(
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now + 100, logicaltime=0, ),
                    ops=[smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.PUT, table=56, tableName="history",
                        index=1, cicada_key_cols=[key2], key=str(
                            key2
                        ).encode(), value=str(key2).encode(), )]
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
                        cmd=smdbrpc_pb2.PUT, table=55, tableName="customer",
                        index=1, cicada_key_cols=[key1, key1, key1], key=str(
                            key1
                        ).encode(), value=str(key1 + 1).encode(), )]
                ), smdbrpc_pb2.TxnReq(
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now + 200, logicaltime=0, ),
                    ops=[smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.PUT, table=56, tableName="history",
                        index=1, cicada_key_cols=[key2], key=str(key2).encode(),
                        value=str(key2 + 1).encode(), )]
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
                        cmd=smdbrpc_pb2.GET, table=55, tableName="customer",
                        index=1, cicada_key_cols=[key1, key1, key1], key=str(
                            key1
                        ).encode(), ), smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.GET, table=56, tableName="history",
                        index=1, cicada_key_cols=[key2, key2, key2, key2],
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
                        cmd=smdbrpc_pb2.PUT, table=57, tableName="neworder",
                        index=1, cicada_key_cols=[key1, key1, key1], key=str(
                            key1
                        ).encode(), value=str(key1).encode(), ), smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.PUT, table=58, tableName="order",
                        index=1, cicada_key_cols=[key2, key2, key2], key=str(
                            key2
                        ).encode(), value=str(key2).encode(), )]
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
                        cmd=smdbrpc_pb2.PUT, table=57, tableName="neworder",
                        index=1, cicada_key_cols=[key1, key1, key1], key=str(
                            key1
                        ).encode(), value=str(key1 + 1).encode()
                    ), ]
                )]
            )
        )
        self.assertEqual(1, len(response.txnResps))
        self.assertTrue(response.txnResps[0].is_committed)

        def follow_put():
            time.sleep(2)
            response = self.stub.BatchSendTxns(
                smdbrpc_pb2.BatchSendTxnsReq(
                    txns=[smdbrpc_pb2.TxnReq(
                        timestamp=smdbrpc_pb2.HLCTimestamp(
                            walltime=self.now + 2000, logicaltime=0, ),
                        ops=[smdbrpc_pb2.Op(
                            cmd=smdbrpc_pb2.PUT, table=57, tableName="neworder",
                            index=1, cicada_key_cols=[key1, key1, key1], key=str(
                                key1
                            ).encode(), value=str(key1 + 1).encode()
                        ), ]
                    )]
                )
            )
            self.assertEqual(1, len(response.txnResps))
            self.assertTrue(response.txnResps[0].is_committed)

        t = threading.Thread(target=follow_put)
        t.start()

        response = self.stub.BatchSendTxns(
            smdbrpc_pb2.BatchSendTxnsReq(
                txns=[smdbrpc_pb2.TxnReq(
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now + 300, logicaltime=0, ),
                    ops=[smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.GET, table=57, tableName="neworder",
                        index=1, cicada_key_cols=[key1, key1, key1],
                        key=str(key1).encode(), ), smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.GET, table=58, tableName="order",
                        index=1, cicada_key_cols=[key2, key2, key2],
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
                        cmd=smdbrpc_pb2.PUT, table=59, tableName="orderline",
                        index=1, cicada_key_cols=[key1, key1, key1, key1],
                        key=str(
                            key1
                        ).encode(), value=str(key1).encode(), ), smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.PUT, table=60, tableName="item",
                        index=1, cicada_key_cols=[key2], key=str(key2).encode(),
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
                        cmd=smdbrpc_pb2.PUT, table=59, tableName="orderline",
                        index=1, cicada_key_cols=[key1, key1, key1, key1],
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
                        cmd=smdbrpc_pb2.PUT, table=59, tableName="orderline",
                        index=1, cicada_key_cols=[key1, key1, key1, key1],
                        key=str(key1).encode(), value=str(key1 + 10).encode(),

                    ), smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.PUT, table=60, tableName="item",
                        index=1, cicada_key_cols=[key2], key=str(key2).encode(),
                        value=str(key2 + 10).encode(), )]
                )]
            )
        )
        self.assertEqual(1, len(response.txnResps))
        self.assertFalse(response.txnResps[0].is_committed)

        def follow_put():
            time.sleep(2)
            response = self.stub.BatchSendTxns(
                smdbrpc_pb2.BatchSendTxnsReq(
                    txns=[smdbrpc_pb2.TxnReq(
                        timestamp=smdbrpc_pb2.HLCTimestamp(
                            walltime=self.now + 1500, logicaltime=0, ),
                        ops=[smdbrpc_pb2.Op(
                            cmd=smdbrpc_pb2.PUT, table=59, tableName="orderline",
                            index=1, cicada_key_cols=[key1, key1, key1, key1],
                            key=str(key1).encode(), value=str(key1 + 10).encode(),

                        ), smdbrpc_pb2.Op(
                            cmd=smdbrpc_pb2.PUT, table=60, tableName="item",
                            index=1, cicada_key_cols=[key2], key=str(key2).encode(),
                            value=str(key2 + 10).encode(), )]
                    )]
                )
            )
            self.assertEqual(1, len(response.txnResps))
            self.assertTrue(response.txnResps[0].is_committed)

        t = threading.Thread(target=follow_put)
        t.start()

        response = self.stub.BatchSendTxns(
            smdbrpc_pb2.BatchSendTxnsReq(
                txns=[smdbrpc_pb2.TxnReq(
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now + 300, logicaltime=0, ),
                    ops=[smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.GET, table=59, tableName="orderline",
                        index=1, cicada_key_cols=[key1, key1, key1, key1],
                        key=str(key1).encode(), ), smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.GET, table=60, tableName="item",
                        index=1, cicada_key_cols=[key2],
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
