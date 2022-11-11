import threading
import unittest
import smdbrpc_pb2
import grpc
import time
import smdbrpc_pb2_grpc


class TestReplicationState(unittest.TestCase):

    def setUp(self):
        self.base_port = 60061
        self.threads = 16

        self.channels = []
        self.stubs = []
        for i in range(self.threads):
            port = self.base_port + i
            self.channels.append(grpc.insecure_channel("localhost:"+str(port)))
            self.stubs.append(smdbrpc_pb2_grpc.HotshardGatewayStub(self.channels[i]))

        self.bg_channel = grpc.insecure_channel("localhost:" + str(self.base_port+self.threads))
        self.bg_stub = smdbrpc_pb2_grpc.HotshardGatewayStub(self.bg_channel)

        self.now = time.time_ns() + 500000000
        self.i = 0

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
        promotionResp = self.stubs[0].PromoteKeysToCicada(promotionReq)
        self.assertEqual(
            len(promotionReq.keys), len(
                promotionResp.successfullyPromoted

            )
        )
        for promoted in promotionResp.successfullyPromoted:
            self.assertTrue(promoted)

        time.sleep(2)
        self.now = time.time_ns()

    def tearDown(self) -> None:
        for i in range(self.threads):
            self.channels[i].close()

    def next(self):
        i = self.i
        self.i += 1
        if self.i > self.threads - 1:
            self.i = 0

        return i

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

        self.stubs[self.next()].ReplicateLog(
            smdbrpc_pb2.ReplicateLogReq(
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
                            ).encode(), value=str(key3).encode(), ), ], ), ],
                currentRT=smdbrpc_pb2.HLCTimestamp(
                    walltime=0,
                    logicaltime=0,
                )
            )
        )

        self.bg_stub.ReplayOnTail(smdbrpc_pb2.ReplayOnTailReq(
            global_watermark=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now + 150,
                logicaltime=0,
            )
        ))

        print("checkBackupState...")
        response = self.stubs[self.next()].CheckBackupState(
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

        self.stubs[self.next()].ReplicateLog(
            smdbrpc_pb2.ReplicateLogReq(
                txns=[smdbrpc_pb2.TxnReq(
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now + 300, logicaltime=0, ),
                    ops=[smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.PUT, table=53, tableName="warehouse",
                        index=1, cicada_key_cols=[key1],
                        key=str(key1).encode(), ), ]
                )],
                currentRT=smdbrpc_pb2.HLCTimestamp(
                    walltime=0,
                    logicaltime=0,
                )
            )
        )

        self.stubs[self.next()].ReplicateLog(
            smdbrpc_pb2.ReplicateLogReq(
                txns=[smdbrpc_pb2.TxnReq(
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now + 400, logicaltime=0, ),
                    ops=[smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.PUT, table=53, tableName="warehouse",
                        index=1, cicada_key_cols=[key1],
                        key=str(key1).encode(), ), ]
                )],
                currentRT=smdbrpc_pb2.HLCTimestamp(
                    walltime=0,
                    logicaltime=0,
                )
            )
        )

        self.bg_stub.ReplayOnTail(smdbrpc_pb2.ReplayOnTailReq(
            global_watermark = smdbrpc_pb2.HLCTimestamp(
                walltime=self.now+500,
                logicaltime=0,
            )
        ))

    def test_succeed_on_non_recent_read(self):
        key1 = 994814
        key2 = 200605
        self.stubs[self.next()].ReplicateLog(
            smdbrpc_pb2.ReplicateLogReq(
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
                )],
                currentRT=smdbrpc_pb2.HLCTimestamp(
                    walltime=0,
                    logicaltime=0,
                )
            )
        )

        self.stubs[self.next()].ReplicateLog(
            smdbrpc_pb2.ReplicateLogReq(
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
                )],
                currentRT=smdbrpc_pb2.HLCTimestamp(
                    walltime=0,
                    logicaltime=0,
                )
            )
        )

        self.bg_stub.ReplayOnTail(smdbrpc_pb2.ReplayOnTailReq(
            global_watermark=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now+300,
                logicaltime=0
            )
        ))

        response = self.stubs[self.next()].CheckBackupState(
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
        self.stubs[self.next()].ReplicateLog(
            smdbrpc_pb2.ReplicateLogReq(
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
                )],
                currentRT=smdbrpc_pb2.HLCTimestamp(
                    walltime=0,
                    logicaltime=0,
                )
            )
        )

        self.stubs[self.next()].ReplicateLog(
            smdbrpc_pb2.ReplicateLogReq(
                txns=[smdbrpc_pb2.TxnReq(
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now + 200, logicaltime=0, ),
                    ops=[smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.PUT, table=57, tableName="neworder",
                        index=1, cicada_key_cols=[key1, key1, key1], key=str(
                            key1
                        ).encode(), value=str(key1 + 1).encode()
                    ), ]
                )],
                currentRT=smdbrpc_pb2.HLCTimestamp(
                    walltime=0,
                    logicaltime=0,
                )
            )
        )
        self.bg_stub.ReplayOnTail(smdbrpc_pb2.ReplayOnTailReq(
            global_watermark=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now+250,
                logicaltime=0,
            )
        ))

        response = self.stubs[self.next()].CheckBackupState(
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

    def test_succeed_on_write_under(self):
        key1 = 994816
        key2 = 200607
        self.stubs[self.next()].ReplicateLog(
            smdbrpc_pb2.ReplicateLogReq(
                txns=[smdbrpc_pb2.TxnReq(
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now + 100, logicaltime=0, ),
                    ops=[smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.PUT, table=59, tableName="orderline",
                        index=1, cicada_key_cols=[key1, key1, key1, key1],
                        key=str(key1).encode(), value=str(key1).encode(), ),
                        smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.PUT, table=60, tableName="item",
                        index=1, cicada_key_cols=[key2], key=str(key2).encode(),
                        value=str(key2).encode(), )]
                )],
                currentRT=smdbrpc_pb2.HLCTimestamp(
                    walltime=0,
                    logicaltime=0,
                )
            )
        )

        self.stubs[self.next()].ReplicateLog(
            smdbrpc_pb2.ReplicateLogReq(
                txns=[smdbrpc_pb2.TxnReq(
                    timestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=self.now + 200, logicaltime=0, ),
                    ops=[smdbrpc_pb2.Op(
                        cmd=smdbrpc_pb2.PUT, table=59, tableName="orderline",
                        index=1, cicada_key_cols=[key1, key1, key1, key1],
                        key=str(key1).encode(), value=str(key1 + 1).encode(), )]
                )],
                currentRT=smdbrpc_pb2.HLCTimestamp(
                    walltime=0,
                    logicaltime=0,
                )
            )
        )

        self.stubs[self.next()].ReplicateLog(
            smdbrpc_pb2.ReplicateLogReq(
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
                )],
                currentRT=smdbrpc_pb2.HLCTimestamp(
                    walltime=0,
                    logicaltime=0,
                )
            )
        )

        self.bg_stub.ReplayOnTail(smdbrpc_pb2.ReplayOnTailReq(
            global_watermark=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now+250,
                logicaltime=0,
            )
        ))

        response = self.stubs[self.next()].CheckBackupState(
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
            str(key2 + 10).encode(), response.txnResps[0].responses[1].value
        )


if __name__ == '__main__':
    unittest.main()