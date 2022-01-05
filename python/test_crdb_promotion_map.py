import time

import grpc

import smdbrpc_pb2
import smdbrpc_pb2_grpc

import unittest


class TestCRDBPromotionMap(unittest.TestCase):

    def setUp(self):
        self.channel = grpc.insecure_channel("localhost:50055")
        self.stub = smdbrpc_pb2_grpc.HotshardGatewayStub(self.channel)
        self.now = time.time_ns()

    def tearDown(self):
        self.channel.close()

    def test_nonexistent_key_returns_false(self):
        key = 1994214
        response = self.stub.TestIsKeyInPromotionMap(
            smdbrpc_pb2.TestPromotionKeyReq(
                key=str(key).encode(),
                promotionTimestamp=smdbrpc_pb2.HLCTimestamp(
                    walltime=self.now + 10,
                    logicaltime=0,
                ),
                cicada_key_cols=[214],
            )
        )
        self.assertFalse(response.isKeyIn)

    def test_add_key_simple_case(self):
        key=1994215
        response = self.stub.TestAddKeyToPromotionMap(
            smdbrpc_pb2.TestPromotionKeyReq(
                key=str(key).encode(),
                promotionTimestamp=smdbrpc_pb2.HLCTimestamp(
                    walltime=self.now + 10,
                    logicaltime=0,
                ),
                cicada_key_cols=[214],
            )
        )
        self.assertTrue(response.isKeyIn)

        response = self.stub.TestIsKeyInPromotionMap(
            smdbrpc_pb2.TestPromotionKeyReq(
                key=str(key).encode(),
                promotionTimestamp=smdbrpc_pb2.HLCTimestamp(
                    walltime=self.now + 20,
                    logicaltime=0,
                )
            )
        )
        self.assertTrue(response.isKeyIn)
        self.assertEqual(1, len(response.cicada_key_cols))
        self.assertEqual(214, response.cicada_key_cols[0])

    def test_smaller_commit_false(self):
        key=1994216
        response = self.stub.TestAddKeyToPromotionMap(
            smdbrpc_pb2.TestPromotionKeyReq(
                key=str(key).encode(),
                promotionTimestamp=smdbrpc_pb2.HLCTimestamp(
                    walltime=self.now + 10,
                    logicaltime=0,
                )
            )
        )
        self.assertTrue(response.isKeyIn)

        response = self.stub.TestIsKeyInPromotionMap(
            smdbrpc_pb2.TestPromotionKeyReq(
                key=str(key).encode(),
                promotionTimestamp=smdbrpc_pb2.HLCTimestamp(
                    walltime=self.now,
                    logicaltime=0,
                )
            )
        )
        self.assertFalse(response.isKeyIn)

    def test_equal_commit_false(self):
        key=1994217
        response = self.stub.TestAddKeyToPromotionMap(
            smdbrpc_pb2.TestPromotionKeyReq(
                key=str(key).encode(),
                promotionTimestamp=smdbrpc_pb2.HLCTimestamp(
                    walltime=self.now + 10,
                    logicaltime=0,
                )
            )
        )
        self.assertTrue(response.isKeyIn)

        response = self.stub.TestIsKeyInPromotionMap(
            smdbrpc_pb2.TestPromotionKeyReq(
                key=str(key).encode(),
                promotionTimestamp=smdbrpc_pb2.HLCTimestamp(
                    walltime=self.now + 10,
                    logicaltime=0,
                )
            )
        )
        self.assertFalse(response.isKeyIn)


if __name__ == '__main__':
    unittest.main()
