import time

import grpc

import smdbrpc_pb2
import smdbrpc_pb2_grpc

import unittest


class TestCRDBDemotion(unittest.TestCase):
    def setUp(self):
        self.channel = grpc.insecure_channel("localhost:50055")
        self.stub = smdbrpc_pb2_grpc.HotshardGatewayStub(self.channel)

        # write a key to be demoted
        self.key = str(1994214).encode()
        value = self.key
        writeTxn = smdbrpc_pb2.CRDBTxnReq(
            ops=[
                smdbrpc_pb2.Op(
                    cmd=smdbrpc_pb2.PUT,
                    key=self.key,
                    value=value,
                )
            ]
        )
        writeTxnResp = self.stub.TestSendTxn(writeTxn)
        self.assertTrue(writeTxnResp.is_committed)

        promotionReq = smdbrpc_pb2.TestPromotionKeyReq(
            key=self.key,
            promotionTimestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=time.time_ns(),
                logicaltime=0,
            )
        )
        promotionResp = self.stub.TestAddKeyToPromotionMap(promotionReq)
        self.assertTrue(promotionResp.isKeyIn)

    # does the demoted key show up in CRDB after its demotion?
    def testDemotedKeyExistsInCRDB(self):
        demotionKey = self.key
        demotionVal = "demotedkey".encode()
        demotionReq = smdbrpc_pb2.KeyMigrationReq(
            key=smdbrpc_pb2.Key(
                key=demotionKey,
            ),
            value=demotionVal,
            isTest=True,
        )
        demotionResp = self.stub.DemoteKey(demotionReq)
        self.assertTrue(demotionResp.is_successfully_migrated)

        readTxn = smdbrpc_pb2.CRDBTxnReq(
            ops=[
                smdbrpc_pb2.Op(
                    cmd=smdbrpc_pb2.GET,
                    key=demotionKey,
                )
            ],
        )
        readTxnResp = self.stub.TestSendTxn(readTxn)
        self.assertTrue(readTxnResp.is_committed)
        self.assertEqual(1, len(readTxnResp.responses))
        self.assertEqual(demotionVal, readTxnResp.responses[0].value)

    # does the demoted key no longer exist in the promotion map?
    def testDemotedKeyNotExistsOnPromotionMap(self):
        demotionKey = self.key
        demotionVal = "demotedkey".encode()
        demotionReq = smdbrpc_pb2.KeyMigrationReq(
            key=smdbrpc_pb2.Key(
                key=demotionKey,
            ),
            value=demotionVal,
            isTest=True,
        )
        demotionResp = self.stub.DemoteKey(demotionReq)
        self.assertTrue(demotionResp.is_successfully_migrated)

        inPromoMapReq = smdbrpc_pb2.TestPromotionKeyReq(
            key=demotionKey,
            promotionTimestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=time.time_ns(),
                logicaltime=0,
            )
        )
        inPromoMapResp = self.stub.TestIsKeyInPromotionMap(inPromoMapReq)
        self.assertFalse(inPromoMapResp.isKeyIn)


class TestCRDBTxns(unittest.TestCase):
    def setUp(self):
        self.channel = grpc.insecure_channel("localhost:50055")
        self.stub = smdbrpc_pb2_grpc.HotshardGatewayStub(self.channel)

    def testBasicWriteFollowedByRead(self):
        key = str(1994215).encode()
        value = key
        writeTxn = smdbrpc_pb2.CRDBTxnReq(
            ops=[
                smdbrpc_pb2.Op(
                    cmd=smdbrpc_pb2.PUT,
                    key=key,
                    value=value,
                ),
            ],
        )
        writeTxnResp = self.stub.TestSendTxn(writeTxn)
        self.assertTrue(writeTxnResp.is_committed)

        readTxn = smdbrpc_pb2.CRDBTxnReq(
            ops=[
                smdbrpc_pb2.Op(
                    cmd=smdbrpc_pb2.GET,
                    key=key,
                ),
            ],
        )
        readTxnResp = self.stub.TestSendTxn(readTxn)
        self.assertTrue(readTxnResp.is_committed)
        self.assertEqual(1, len(readTxnResp.responses))
        self.assertEqual(value, readTxnResp.responses[0].value)

    def testBasicWriteFollowedByReadWithTimestamps(self):
        key = str(1994214).encode()
        value = key
        writeTxn = smdbrpc_pb2.CRDBTxnReq(
            ops=[
                smdbrpc_pb2.Op(
                    cmd=smdbrpc_pb2.PUT,
                    key=key,
                    value=value,
                ),
            ],
            timestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=time.time_ns(),
                logicaltime=0,
            )
        )
        writeTxnResp = self.stub.TestSendTxn(writeTxn)
        self.assertTrue(writeTxnResp.is_committed)

        readTxn = smdbrpc_pb2.CRDBTxnReq(
            ops=[
                smdbrpc_pb2.Op(
                    cmd=smdbrpc_pb2.GET,
                    key=key,
                ),
            ],
            timestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=time.time_ns(),
                logicaltime=5,
            )
        )
        readTxnResp = self.stub.TestSendTxn(readTxn)
        self.assertTrue(readTxnResp.is_committed)
        self.assertEqual(1, len(readTxnResp.responses))
        self.assertEqual(value, readTxnResp.responses[0].value)

    def testMultiWritesFollowedByReadsWithTimestamps(self):
        key1, key2 = str(1994214).encode(), str(1994812).encode()
        value1, value2 = key1, key2
        writeTxn = smdbrpc_pb2.CRDBTxnReq(
            ops=[
                smdbrpc_pb2.Op(
                    cmd=smdbrpc_pb2.PUT,
                    key=key1,
                    value=value1,
                ),
                smdbrpc_pb2.Op(
                    cmd=smdbrpc_pb2.PUT,
                    key=key2,
                    value=value2,
                ),
            ],
            timestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=time.time_ns(),
                logicaltime=0,
            )
        )
        writeTxnResp = self.stub.TestSendTxn(writeTxn)
        self.assertTrue(writeTxnResp.is_committed)

        readTxn = smdbrpc_pb2.CRDBTxnReq(
            ops=[
                smdbrpc_pb2.Op(
                    cmd=smdbrpc_pb2.GET,
                    key=key1,
                ),
                smdbrpc_pb2.Op(
                    cmd=smdbrpc_pb2.GET,
                    key=key2,
                ),
            ],
            timestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=time.time_ns(),
                logicaltime=5,
            )
        )
        readTxnResp = self.stub.TestSendTxn(readTxn)
        self.assertTrue(readTxnResp.is_committed)
        self.assertEqual(2, len(readTxnResp.responses))
        self.assertEqual(value1, readTxnResp.responses[0].value)
        self.assertEqual(value2, readTxnResp.responses[1].value)

    def testMultiWritesFollowedByReads(self):
        key1, key2 = str(1994214).encode(), str(1994812).encode()
        value1, value2 = key1, key2
        writeTxn = smdbrpc_pb2.CRDBTxnReq(
            ops=[
                smdbrpc_pb2.Op(
                    cmd=smdbrpc_pb2.PUT,
                    key=key1,
                    value=value1,
                ),
                smdbrpc_pb2.Op(
                    cmd=smdbrpc_pb2.PUT,
                    key=key2,
                    value=value2,
                ),
            ],
        )
        writeTxnResp = self.stub.TestSendTxn(writeTxn)
        self.assertTrue(writeTxnResp.is_committed)

        readTxn = smdbrpc_pb2.CRDBTxnReq(
            ops=[
                smdbrpc_pb2.Op(
                    cmd=smdbrpc_pb2.GET,
                    key=key1,
                ),
                smdbrpc_pb2.Op(
                    cmd=smdbrpc_pb2.GET,
                    key=key2,
                ),
            ],
        )
        readTxnResp = self.stub.TestSendTxn(readTxn)
        self.assertTrue(readTxnResp.is_committed)
        self.assertEqual(2, len(readTxnResp.responses))
        self.assertEqual(value1, readTxnResp.responses[0].value)
        self.assertEqual(value2, readTxnResp.responses[1].value)
