import time

import grpc

import smdbrpc_pb2
import smdbrpc_pb2_grpc

import unittest


# class TestCalculateStats(unittest.TestCase):
#     def setUp(self):
#         self.channel = grpc.insecure_channel("localhost:50051")
#         self.stub = smdbrpc_pb2_grpc.HotshardGatewayStub(self.channel)
#         self.now = time.time_ns()
#
#     def tearDown(self) -> None:
#         self.channel.close()
#
#     def testBasicFunctionality(self):
#         print("jenndebug")
#         response = self.stub.CalculateCicadaStats(smdbrpc_pb2.CalculateCicadaReq(
#             cpu_target=0.75,
#             cpu_ceiling=0.85,
#             cpu_floor=0.65,
#             mem_target=0.75,
#             mem_ceiling=0.85,
#             mem_floor=0.65,
#             percentile_n=0.25,
#         ))
#         print("jenndebug finished")
#         self.assertFalse(response.demotion_only)
#         self.assertLess(0, response.qps_avail_for_promotion)
#         self.assertLess(0, response.num_keys_avail_for_promotion)


class TestCicadaSingleKeyTxns(unittest.TestCase):

    def setUp(self):
        self.channel = grpc.insecure_channel("localhost:50051")
        self.stub = smdbrpc_pb2_grpc.HotshardGatewayStub(self.channel)
        self.now = time.time_ns()

    def tearDown(self) -> None:
        self.channel.close()

    def test_simple_read(self):
        """
        Tests that reading an un-inserted value comes back empty.
        """
        key = 1994214
        op = smdbrpc_pb2.Op(
            cmd=smdbrpc_pb2.GET,
            table=53,
            index=1,
            key_cols=[key],
            key=key.to_bytes(64, "big"),
        )
        timestamp = smdbrpc_pb2.HLCTimestamp(
            walltime=self.now + 100,
            logicaltime=0,
        )
        response = self.stub.SendTxn(smdbrpc_pb2.TxnReq(
            ops=[op],
            timestamp=timestamp,
        ))
        self.assertTrue(response.is_committed)
        self.assertEqual(0, len(response.responses))

    def test_simple_write(self):
        key = 994215
        val = "hello".encode()
        op = smdbrpc_pb2.Op(
            cmd=smdbrpc_pb2.PUT,
            table=53,
            index=1,
            key_cols=[key],
            key=val,
            value=val,
        )
        timestamp = smdbrpc_pb2.HLCTimestamp(
            walltime=self.now + 100,
            logicaltime=0,
        )
        response = self.stub.SendTxn(smdbrpc_pb2.TxnReq(
            ops=[op],
            timestamp=timestamp,
        ))
        self.assertTrue(response.is_committed)

        op = smdbrpc_pb2.Op(
            cmd=smdbrpc_pb2.GET,
            table=53,
            index=1,
            key_cols=[key],
            key=val,
        )
        response = self.stub.SendTxn(smdbrpc_pb2.TxnReq(
            ops=[op],
            timestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now + 200,
                logicaltime=0,
            ),
        ))
        self.assertTrue(response.is_committed)
        self.assertEqual(1, len(response.responses))
        self.assertEqual(val, response.responses[0].value)

    def test_fail_on_write_under_read(self):
        key = 994216
        response = self.stub.SendTxn(smdbrpc_pb2.TxnReq(
            ops=[smdbrpc_pb2.Op(
                cmd=smdbrpc_pb2.PUT,
                table=53,
                index=1,
                key_cols=[key],
                key=str(key).encode(),
                value=str(key).encode(),
            )],
            timestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now + 100,
                logicaltime=0,
            )
        ))
        self.assertTrue(response.is_committed)
        response = self.stub.SendTxn(smdbrpc_pb2.TxnReq(
            ops=[smdbrpc_pb2.Op(
                cmd=smdbrpc_pb2.GET,
                table=53,
                index=1,
                key_cols=[key],
                key=str(key).encode(),
            )],
            timestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now + 200,
                logicaltime=0,
            )
        ))
        self.assertTrue(response.is_committed)
        self.assertEqual(1, len(response.responses))
        self.assertEqual(str(key).encode(), response.responses[0].value)
        response = self.stub.SendTxn(smdbrpc_pb2.TxnReq(
            ops=[smdbrpc_pb2.Op(
                cmd=smdbrpc_pb2.PUT,
                table=53,
                index=1,
                key_cols=[key],
                key=str(key).encode(),
                value=str(key + 1).encode(),
            )],
            timestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now + 150,
                logicaltime=0,
            )
        ))
        self.assertFalse(response.is_committed)
        response = self.stub.SendTxn(smdbrpc_pb2.TxnReq(
            ops=[smdbrpc_pb2.Op(
                cmd=smdbrpc_pb2.GET,
                table=53,
                index=1,
                key_cols=[key],
                key=str(key).encode(),
            )],
            timestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now + 300,
                logicaltime=0,
            )
        ))
        self.assertTrue(response.is_committed)
        self.assertTrue(1, len(response.responses))
        self.assertEqual(str(key).encode(), response.responses[0].value)

    def test_succeed_on_non_recent_read(self):
        key = 994217
        response = self.stub.SendTxn(smdbrpc_pb2.TxnReq(
            timestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now + 100,
                logicaltime=0,
            ),
            ops=[smdbrpc_pb2.Op(
                cmd=smdbrpc_pb2.PUT,
                table=53,
                index=1,
                key_cols=[key],
                key=str(key).encode(),
                value=str(key).encode(),
            )]
        ))
        self.assertTrue(response.is_committed)
        response = self.stub.SendTxn(smdbrpc_pb2.TxnReq(
            timestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now + 200,
                logicaltime=0,
            ),
            ops=[smdbrpc_pb2.Op(
                cmd=smdbrpc_pb2.PUT,
                table=53,
                index=1,
                key_cols=[key],
                key=str(key).encode(),
                value=str(key + 1).encode(),
            )]
        ))
        self.assertTrue(response.is_committed)
        response = self.stub.SendTxn(smdbrpc_pb2.TxnReq(
            timestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now + 150,
                logicaltime=0,
            ),
            ops=[smdbrpc_pb2.Op(
                cmd=smdbrpc_pb2.GET,
                table=53,
                index=1,
                key_cols=[key],
                key=str(key).encode(),
            )]
        ))
        self.assertTrue(response.is_committed)
        self.assertTrue(1, len(response.responses))
        self.assertEqual(str(key).encode(), response.responses[0].value)

    def test_fail_on_write_under_write(self):
        key = 994218
        response = self.stub.SendTxn(smdbrpc_pb2.TxnReq(
            timestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now + 100,
                logicaltime=0,
            ),
            ops=[smdbrpc_pb2.Op(
                cmd=smdbrpc_pb2.PUT,
                table=53,
                index=1,
                key_cols=[key],
                key=str(key).encode(),
                value=str(key).encode(),
            )],
        ))
        self.assertTrue(response.is_committed)
        response = self.stub.SendTxn(smdbrpc_pb2.TxnReq(
            timestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now + 200,
                logicaltime=0,
            ),
            ops=[smdbrpc_pb2.Op(
                cmd=smdbrpc_pb2.PUT,
                table=53,
                index=1,
                key_cols=[key],
                key=str(key).encode(),
                value=str(key + 1).encode(),
            )],
        ))
        self.assertTrue(response.is_committed)
        response = self.stub.SendTxn(smdbrpc_pb2.TxnReq(
            timestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now + 150,
                logicaltime=0,
            ),
            ops=[smdbrpc_pb2.Op(
                cmd=smdbrpc_pb2.PUT,
                table=53,
                index=1,
                key_cols=[key],
                key=str(key).encode(),
                value=str(key + 10).encode(),
            )],
        ))
        self.assertFalse(response.is_committed)
        response = self.stub.SendTxn(smdbrpc_pb2.TxnReq(
            timestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now + 300,
                logicaltime=0,
            ),
            ops=[smdbrpc_pb2.Op(
                cmd=smdbrpc_pb2.GET,
                table=53,
                index=1,
                key_cols=[key],
                key=str(key).encode(),
            )],
        ))
        self.assertTrue(response.is_committed)
        self.assertEqual(1, len(response.responses))
        self.assertEqual(str(key + 1).encode(), response.responses[0].value)


class TestCicadaMultiKeyTxns(unittest.TestCase):

    def setUp(self):
        self.channel = grpc.insecure_channel("localhost:50051")
        self.stub = smdbrpc_pb2_grpc.HotshardGatewayStub(self.channel)
        self.now = time.time_ns()

    def tearDown(self) -> None:
        self.channel.close()

    def test_simple_read(self):
        key1 = 1994812
        key2 = 20200603
        response = self.stub.SendTxn(smdbrpc_pb2.TxnReq(
            timestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now + 100,
                logicaltime=0,
            ),
            ops=[
                smdbrpc_pb2.Op(
                    cmd=smdbrpc_pb2.GET,
                    table=53,
                    index=1,
                    key_cols=[key1],
                    key=str(key1).encode(),
                ),
                smdbrpc_pb2.Op(
                    cmd=smdbrpc_pb2.GET,
                    table=53,
                    index=1,
                    key_cols=[key2],
                    key=str(key2).encode(),
                )
            ]
        ))
        self.assertTrue(response.is_committed)
        self.assertEqual(0, len(response.responses))

    def test_simple_write(self):
        key1 = 994813
        key2 = 200604
        response = self.stub.SendTxn(smdbrpc_pb2.TxnReq(
            timestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now + 100,
                logicaltime=0,
            ),
            ops=[
                smdbrpc_pb2.Op(
                    cmd=smdbrpc_pb2.PUT,
                    table=53,
                    index=1,
                    key_cols=[key1],
                    key=str(key1).encode(),
                    value=str(key1).encode(),
                ),
                smdbrpc_pb2.Op(
                    cmd=smdbrpc_pb2.PUT,
                    table=53,
                    index=1,
                    key_cols=[key2],
                    key=str(key2).encode(),
                    value=str(key2).encode(),
                ),
            ]
        ))
        self.assertTrue(response.is_committed)
        response = self.stub.SendTxn(smdbrpc_pb2.TxnReq(
            timestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now + 200,
                logicaltime=0,
            ),
            ops=[
                smdbrpc_pb2.Op(
                    cmd=smdbrpc_pb2.GET,
                    table=53,
                    index=1,
                    key_cols=[key1],
                    key=str(key1).encode(),
                ),
                smdbrpc_pb2.Op(
                    cmd=smdbrpc_pb2.GET,
                    table=53,
                    index=1,
                    key_cols=[key2],
                    key=str(key2).encode(),
                )
            ]
        ))
        self.assertTrue(response.is_committed)
        self.assertEqual(2, len(response.responses))
        self.assertEqual(str(key1).encode(), response.responses[0].value)
        self.assertEqual(str(key2).encode(), response.responses[1].value)

    def test_succeed_on_non_recent_read(self):
        key1 = 994814
        key2 = 200605
        response = self.stub.SendTxn(smdbrpc_pb2.TxnReq(
            timestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now + 100,
                logicaltime=0,
            ),
            ops=[
                smdbrpc_pb2.Op(
                    cmd=smdbrpc_pb2.PUT,
                    table=53,
                    index=1,
                    key_cols=[key1],
                    key=str(key1).encode(),
                    value=str(key1).encode(),
                ),
                smdbrpc_pb2.Op(
                    cmd=smdbrpc_pb2.PUT,
                    table=53,
                    index=1,
                    key_cols=[key2],
                    key=str(key2).encode(),
                    value=str(key2).encode(),
                )
            ]
        ))
        self.assertTrue(response.is_committed)
        response = self.stub.SendTxn(smdbrpc_pb2.TxnReq(
            timestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now+200,
                logicaltime=0,
            ),
            ops=[
                smdbrpc_pb2.Op(
                    cmd=smdbrpc_pb2.PUT,
                    table=53,
                    index=1,
                    key_cols=[key1],
                    key=str(key1).encode(),
                    value=str(key1+1).encode(),
                )
            ]
        ))
        self.assertTrue(response.is_committed)
        response = self.stub.SendTxn(smdbrpc_pb2.TxnReq(
            timestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now+150,
                logicaltime=0,
            ),
            ops=[
                smdbrpc_pb2.Op(
                    cmd=smdbrpc_pb2.GET,
                    table=53,
                    index=1,
                    key_cols=[key1],
                    key=str(key1).encode(),
                ),
                smdbrpc_pb2.Op(
                    cmd=smdbrpc_pb2.GET,
                    table=53,
                    index=1,
                    key_cols=[key2],
                    key=str(key2).encode(),
                ),
            ]
        ))
        self.assertTrue(response.is_committed)
        self.assertEqual(2, len(response.responses))
        self.assertEqual(str(key1).encode(), response.responses[0].value)
        self.assertEqual(str(key2).encode(), response.responses[1].value)

    def test_succeed_on_disjoint_updates(self):
        key1 = 994815
        key2 = 200606
        response = self.stub.SendTxn(smdbrpc_pb2.TxnReq(
            timestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now+100,
                logicaltime=0,
            ),
            ops=[
                smdbrpc_pb2.Op(
                    cmd=smdbrpc_pb2.PUT,
                    table=53,
                    index=1,
                    key_cols=[key1],
                    key=str(key1).encode(),
                    value=str(key1).encode(),
                ),
                smdbrpc_pb2.Op(
                    cmd=smdbrpc_pb2.PUT,
                    table=53,
                    index=1,
                    key_cols=[key2],
                    key=str(key2).encode(),
                    value=str(key2).encode(),
                )
            ]
        ))
        self.assertTrue(response.is_committed)
        response = self.stub.SendTxn(smdbrpc_pb2.TxnReq(
            timestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now+200,
                logicaltime=0,
            ),
            ops=[
                smdbrpc_pb2.Op(
                    cmd=smdbrpc_pb2.PUT,
                    table=53,
                    index=1,
                    key_cols=[key1],
                    key=str(key1).encode(),
                    value=str(key1+1).encode()
                ),
            ]
        ))
        self.assertTrue(response.is_committed)
        response = self.stub.SendTxn(smdbrpc_pb2.TxnReq(
            timestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now+300,
                logicaltime=0,
            ),
            ops=[
                smdbrpc_pb2.Op(
                    cmd=smdbrpc_pb2.GET,
                    table=53,
                    index=1,
                    key_cols=[key1],
                    key=str(key1).encode(),
                ),
                smdbrpc_pb2.Op(
                    cmd=smdbrpc_pb2.GET,
                    table=53,
                    index=1,
                    key_cols=[key2],
                    key=str(key2).encode(),
                ),
            ],
        ))
        self.assertTrue(response.is_committed)
        self.assertEqual(2, len(response.responses))
        self.assertEqual(str(key1 + 1).encode(), response.responses[0].value)
        self.assertEqual(str(key2).encode(), response.responses[1].value)

    def test_fail_on_write_between_disjoint_updates(self):
        key1 = 994816
        key2 = 200607
        response = self.stub.SendTxn(smdbrpc_pb2.TxnReq(
            timestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now+100,
                logicaltime=0,
            ),
            ops=[
                smdbrpc_pb2.Op(
                    cmd=smdbrpc_pb2.PUT,
                    table=53,
                    index=1,
                    key_cols=[key1],
                    key=str(key1).encode(),
                    value=str(key1).encode(),
                ),
                smdbrpc_pb2.Op(
                    cmd=smdbrpc_pb2.PUT,
                    table=53,
                    index=1,
                    key_cols=[key2],
                    key=str(key2).encode(),
                    value=str(key2).encode(),
                )
            ]
        ))
        self.assertTrue(response.is_committed)
        response= self.stub.SendTxn(smdbrpc_pb2.TxnReq(
            timestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now+200,
                logicaltime=0,
            ),
            ops=[
                smdbrpc_pb2.Op(
                    cmd=smdbrpc_pb2.PUT,
                    table=53,
                    index=1,
                    key_cols=[key1],
                    key=str(key1).encode(),
                    value=str(key1+1).encode(),
                )
            ]
        ))
        self.assertTrue(response.is_committed)
        response = self.stub.SendTxn(smdbrpc_pb2.TxnReq(
            timestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now+150,
                logicaltime=0,
            ),
            ops=[
                smdbrpc_pb2.Op(
                    cmd=smdbrpc_pb2.PUT,
                    table=53,
                    index=1,
                    key_cols=[key1],
                    key=str(key1).encode(),
                    value=str(key1+10).encode(),

                ),
                smdbrpc_pb2.Op(
                    cmd=smdbrpc_pb2.PUT,
                    table=53,
                    index=1,
                    key_cols=[key2],
                    key=str(key2).encode(),
                    value=str(key2+10).encode(),
                )
            ]
        ))
        self.assertFalse(response.is_committed)
        response = self.stub.SendTxn(smdbrpc_pb2.TxnReq(
            timestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now+300,
                logicaltime=0,
            ),
            ops=[
                smdbrpc_pb2.Op(
                    cmd=smdbrpc_pb2.GET,
                    table=53,
                    index=1,
                    key_cols=[key1],
                    key=str(key1).encode(),
                ),
                smdbrpc_pb2.Op(
                    cmd=smdbrpc_pb2.GET,
                    table=53,
                    index=1,
                    key_cols=[key2],
                    key=str(key2).encode(),
                )
            ]
        ))
        self.assertTrue(response.is_committed)
        self.assertEqual(2, len(response.responses))
        self.assertEqual(str(key1 + 1).encode(), response.responses[0].value)
        self.assertEqual(str(key2).encode(), response.responses[1].value)


if __name__ == '__main__':
    unittest.main()
