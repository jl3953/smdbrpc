import time

import grpc

import smdbrpc_pb2
import smdbrpc_pb2_grpc

import unittest


class TestTriggerDemotionByNums(unittest.TestCase):
    def setUp(self):
        self.channel = grpc.insecure_channel("localhost:50051")
        self.stub = smdbrpc_pb2_grpc.HotshardGatewayStub(self.channel)

    def tearDown(self) -> None:
        self.channel.close()

    def testNoDemotionWhenZeroExtraResources(self):
        zeroQpsReq = smdbrpc_pb2.TriggerDemotionByNumsReq(
            qps_in_excess=0,
            num_keys_in_excess=100,
            demotion_timestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=time.time_ns(),
                logicaltime=0,
            ),
            isTest=True,
        )
        zeroQpsResp = self.stub.TriggerDemotionByNums(zeroQpsReq)
        self.assertTrue(zeroQpsResp.were_demotions_triggered)
        self.assertEqual(0, zeroQpsResp.qps_demoted)
        self.assertEqual(0, zeroQpsResp.num_keys_demoted)

        zeroMemReq = smdbrpc_pb2.TriggerDemotionByNumsReq(
            qps_in_excess=100,
            num_keys_in_excess=0,
            demotion_timestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=time.time_ns(),
                logicaltime=0,
            ),
            isTest=True,
        )
        zeroMemResp = self.stub.TriggerDemotionByNums(zeroMemReq)
        self.assertTrue(zeroMemResp.were_demotions_triggered)
        self.assertEqual(0, zeroMemResp.qps_demoted)
        self.assertEqual(0, zeroMemResp.num_keys_demoted)

    def testDemotesAtAll(self):
        req = smdbrpc_pb2.TriggerDemotionByNumsReq(
            qps_in_excess=100,
            num_keys_in_excess=100,
            demotion_timestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=time.time_ns(),
                logicaltime=0,
            ),
            isTest=True,
        )
        resp = self.stub.TriggerDemotionByNums(req)
        self.assertTrue(resp.were_demotions_triggered)
        self.assertLess(0, resp.qps_demoted)
        self.assertLess(0, resp.num_keys_demoted)


class TestTriggerDemotion(unittest.TestCase):
    def setUp(self):
        self.channel = grpc.insecure_channel("localhost:50051")
        self.stub = smdbrpc_pb2_grpc.HotshardGatewayStub(self.channel)
        self.now = time.time_ns()

        self.demotedkey = 1994214
        self.demotedval = "hello".encode()
        self.key = smdbrpc_pb2.Key(
            table=53,
            index=1,
            key_cols=[self.demotedkey],
            key=self.demotedval,
            timestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now,
                logicaltime=0,
            )
        )

        # write the key to Cicada
        writeOp = smdbrpc_pb2.Op(
            cmd=smdbrpc_pb2.PUT,
            table=self.key.table,
            index=self.key.index,
            key_cols=self.key.key_cols,
            key=self.key.key,
            value=self.demotedval,
        )
        timestamp = smdbrpc_pb2.HLCTimestamp(
            walltime=self.now - 100,
            logicaltime=0,
        )
        response = self.stub.SendTxn(smdbrpc_pb2.TxnReq(
            ops=[writeOp],
            timestamp=timestamp,
            is_promotion=True,
        ))
        self.assertTrue(response.is_committed)

        # demote the key
        response = self.stub.TriggerDemotion(
            smdbrpc_pb2.TriggerDemotionRequest(
                key=self.key,
                testLocking=False,
                do_not_contact_crdb=True,
            )
        )
        self.assertEqual(1, len(response.triggerDemotionStatuses))
        self.assertTrue(response.triggerDemotionStatuses[0].isDemotionTriggered)

    def tearDown(self) -> None:
        self.channel.close()

    def testWriteTransactionFailsAfterDemotion(self):
        # send a write txn after it to see if it fails
        writeOp = smdbrpc_pb2.Op(
            cmd=smdbrpc_pb2.PUT,
            table=self.key.table,
            index=self.key.index,
            key_cols=self.key.key_cols,
            key=self.key.key,
            value=self.demotedval,
        )
        writeTs = smdbrpc_pb2.HLCTimestamp(
            walltime=self.now + 2000,
            logicaltime=0,
        )
        response = self.stub.SendTxn(
            smdbrpc_pb2.TxnReq(
                ops=[writeOp],
                timestamp=writeTs,
                is_promotion=False,
            )
        )
        self.assertFalse(response.is_committed)

    def testReadTransactionSucceedsBeforeButFailsAfterDemotion(self):
        # send a read txn before it, should succeed
        readOp = smdbrpc_pb2.Op(
            cmd=smdbrpc_pb2.GET,
            table=self.key.table,
            index=self.key.index,
            key_cols=self.key.key_cols,
            key=self.key.key,
        )
        readBeforeTs = smdbrpc_pb2.HLCTimestamp(
            walltime=self.now - 1,
            logicaltime=1,
        )
        readBeforeResponse = self.stub.SendTxn(
            smdbrpc_pb2.TxnReq(
                ops=[readOp],
                timestamp=readBeforeTs,
                is_promotion=False,
            )
        )
        self.assertTrue(readBeforeResponse.is_committed)
        self.assertEqual(1, len(readBeforeResponse.responses))
        self.assertEqual(self.demotedval, readBeforeResponse.responses[0].value)

        # send a read txn after it, should fail
        readAfterTs = smdbrpc_pb2.HLCTimestamp(
            walltime=self.now + 2000,
            logicaltime=0,
        )
        readAfterResponse = self.stub.SendTxn(
            smdbrpc_pb2.TxnReq(
                ops=[readOp],
                timestamp=readAfterTs,
                is_promotion=False,
            )
        )
        if len(readAfterResponse.responses) > 0:
            print(readAfterResponse.responses[0].value)
        self.assertFalse(readAfterResponse.is_committed)

    def testPromotionAfterDemotionSucceeds(self):
        # demote a key
        # send a promotion after it, it should succeed
        writeOp = smdbrpc_pb2.Op(
            cmd=smdbrpc_pb2.PUT,
            table=self.key.table,
            index=self.key.index,
            key_cols=self.key.key_cols,
            key=self.key.key,
            value=self.demotedval,
        )
        writeTs = smdbrpc_pb2.HLCTimestamp(
            walltime=self.now + 2000,
            logicaltime=0,
        )
        response = self.stub.SendTxn(
            smdbrpc_pb2.TxnReq(
                ops=[writeOp],
                timestamp=writeTs,
                is_promotion=True,
            )
        )
        self.assertTrue(response.is_committed)

    def testDemotionAfterDemotionFails(self):
        # demote the key
        key = smdbrpc_pb2.Key(
            table=53,
            index=1,
            key_cols=[self.demotedkey],
            key=self.demotedval,
            timestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now,
                logicaltime=1000,
            )
        )
        response = self.stub.TriggerDemotion(
            smdbrpc_pb2.TriggerDemotionRequest(
                key=key,
                testLocking=False,
                do_not_contact_crdb=True,
            )
        )
        self.assertEqual(1, len(response.triggerDemotionStatuses))
        self.assertTrue(response.triggerDemotionStatuses[0].isDemotionTriggered)


class TestTriggerDemotionLock(unittest.TestCase):
    def setUp(self):
        self.channel = grpc.insecure_channel("localhost:50051")
        self.stub = smdbrpc_pb2_grpc.HotshardGatewayStub(self.channel)
        self.now = time.time_ns()

        self.rawLockedKey = 1994214
        self.demotedval = "hello".encode()
        self.lockedKey = smdbrpc_pb2.Key(
            table=53,
            index=1,
            key_cols=[self.rawLockedKey],
            key=self.demotedval,
            timestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now,
                logicaltime=0,
            )
        )

    def tearDown(self) -> None:
        self.channel.close()

    def testDemote(self):
        channel = grpc.insecure_channel("localhost:50051")
        stub = smdbrpc_pb2_grpc.HotshardGatewayStub(channel)
        writeBeforeLockingOp = smdbrpc_pb2.Op(
            cmd=smdbrpc_pb2.PUT,
            table=self.lockedKey.table,
            index=self.lockedKey.index,
            key_cols=self.lockedKey.key_cols,
            key=self.lockedKey.key,
            value=self.demotedval,
        )

        writeBeforeLockingResp = stub.SendTxn(smdbrpc_pb2.TxnReq(
            ops=[writeBeforeLockingOp],
            timestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now - 1000,
                logicaltime=0,
            ),
            is_promotion=True,
        ))
        self.assertTrue(writeBeforeLockingResp.is_committed)

        _ = stub.TriggerDemotion(
            smdbrpc_pb2.TriggerDemotionRequest(
                key=self.lockedKey,
                testLocking=True,
            )
        )

    def testWrite(self):
        channel = grpc.insecure_channel("localhost:50051")
        stub = smdbrpc_pb2_grpc.HotshardGatewayStub(channel)
        print("connected")
        writeAfterLockOp = smdbrpc_pb2.Op(
            cmd=smdbrpc_pb2.PUT,
            table=self.lockedKey.table,
            index=self.lockedKey.index,
            key_cols=self.lockedKey.key_cols,
            key=self.lockedKey.key,
            value="wellshit".encode(),
        )
        print("sending...")
        _ = stub.SendTxn(
            smdbrpc_pb2.TxnReq(
                ops=[writeAfterLockOp],
                timestamp=smdbrpc_pb2.HLCTimestamp(
                    walltime=self.now + 2000,
                    logicaltime=0,
                ),
            )
        )
        print("returned")

    def testThatLockActuallyWorks(self):
        def demote():
            channel = grpc.insecure_channel("localhost:50051")
            stub = smdbrpc_pb2_grpc.HotshardGatewayStub(channel)
            _ = stub.TriggerDemotion(
                smdbrpc_pb2.TriggerDemotionRequest(
                    key=self.lockedKey,
                    testLocking=True,
                )
            )
            # TODO jenndebug check that key cols match
            # self.assertTrue(response.triggerDemotionStatuses[0].isDemotionTriggered)
            # self.assertEqual(1, len(demotionLockResponse.triggerDemotionStatuses))
            # self.assertTrue(demotionLockResponse.triggerDemotionStatuses[0].isDemotionTriggered)

        import threading
        demotionThread = threading.Thread(target=demote)
        demotionThread.start()
        print("hello")
        time.sleep(2)
        print("goodbye")

        writeAfterLockOp = smdbrpc_pb2.Op(
            cmd=smdbrpc_pb2.PUT,
            table=self.lockedKey.table,
            index=self.lockedKey.index,
            key_cols=self.lockedKey.key_cols,
            key=self.lockedKey.key,
            value="wellshit".encode(),
        )
        print("sending...")
        writeAfterLockResponse = self.stub.SendTxn(
            smdbrpc_pb2.TxnReq(
                ops=[writeAfterLockOp],
                timestamp=smdbrpc_pb2.HLCTimestamp(
                    walltime=self.now + 2000,
                    logicaltime=0,
                ),
            )
        )
        print("allright you work")
        self.assertFalse(writeAfterLockResponse.is_committed)

    def testThatLockActuallyLocksWrong(self):
        # send a trigger demotion request with a single key
        demotionLockResponse = self.stub.TriggerDemotion(
            smdbrpc_pb2.TriggerDemotionRequest(
                key=self.lockedKey,
                testLocking=True,
            )
        )
        # TODO jenndebug check that key cols match
        # self.assertTrue(response.triggerDemotionStatuses[0].isDemotionTriggered)
        self.assertEqual(1, len(demotionLockResponse.triggerDemotionStatuses))
        self.assertTrue(demotionLockResponse.triggerDemotionStatuses[0].isDemotionTriggered)
        time.sleep(2)

        # when that returns successfully, send a SendTxn write to that same key -- should fail
        writeAfterLockOp = smdbrpc_pb2.Op(
            cmd=smdbrpc_pb2.PUT,
            table=self.lockedKey.table,
            index=self.lockedKey.index,
            key_cols=self.lockedKey.key_cols,
            key=self.lockedKey.key,
            value="wellshit".encode(),
        )
        writeAfterLockResponse = self.stub.SendTxn(
            smdbrpc_pb2.TxnReq(
                ops=[writeAfterLockOp],
                timestamp=smdbrpc_pb2.HLCTimestamp(
                    walltime=self.now + 2000,
                    logicaltime=0,
                ),
            )
        )
        self.assertFalse(writeAfterLockResponse.is_committed)
        # time.sleep(2)

        # also SendTxn a read to that key -- should pass with the previous value...?
        # readAfterLockOp = smdbrpc_pb2.Op(
        #     cmd=smdbrpc_pb2.GET,
        #     table=self.lockedKey.table,
        #     index=self.lockedKey.index,
        #     key_cols=self.lockedKey.key_cols,
        #     key=self.lockedKey.key,
        # )
        # readAfterLockTs = smdbrpc_pb2.HLCTimestamp(
        #     walltime=self.now + 3000,
        #     logicaltime=0,
        # )
        # readAfterLockResponse = self.stub.SendTxn(
        #     smdbrpc_pb2.TxnReq(
        #         ops=[readAfterLockOp],
        #         timestamp=readAfterLockTs,
        #     ),
        # )
        # self.assertFalse(readAfterLockResponse.is_committed)


class TestCalculateStats(unittest.TestCase):
    def setUp(self):
        self.channel = grpc.insecure_channel("localhost:50051")
        self.stub = smdbrpc_pb2_grpc.HotshardGatewayStub(self.channel)
        self.now = time.time_ns()

    def tearDown(self) -> None:
        self.channel.close()

    def testDemotionOnly(self):
        response = self.stub.CalculateCicadaStats(smdbrpc_pb2.CalculateCicadaReq(
            cpu_target=0.0,
            cpu_ceiling=0.1,
            cpu_floor=0.0,
            mem_target=0.0,
            mem_ceiling=0.1,
            mem_floor=0.0,
            percentile_n=0.25,
            timestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now,
                logicaltime=0,
            )
        ))
        self.assertTrue(response.demotion_only)

    def testPromotionOnly(self):
        response = self.stub.CalculateCicadaStats(smdbrpc_pb2.CalculateCicadaReq(
            cpu_target=1.0,
            cpu_ceiling=1.0,
            cpu_floor=1.0,
            mem_target=1.0,
            mem_ceiling=1.0,
            mem_floor=1.0,
            percentile_n=0.25,
        ))
        self.assertFalse(response.demotion_only)
        self.assertLess(0, response.qps_avail_for_promotion)
        self.assertLess(0, response.num_keys_avail_for_promotion)

    def testRAMBottleneck(self):
        response = self.stub.CalculateCicadaStats(smdbrpc_pb2.CalculateCicadaReq(
            cpu_target=1.0,
            cpu_ceiling=1.0,
            cpu_floor=1.0,
            mem_target=0,
            mem_ceiling=0,
            mem_floor=0,
            percentile_n=0.25,
        ))
        self.assertTrue(response.demotion_only)

    def testCPUBottleneck(self):
        response = self.stub.CalculateCicadaStats(smdbrpc_pb2.CalculateCicadaReq(
            cpu_target=0,
            cpu_ceiling=0,
            cpu_floor=0,
            mem_target=1.0,
            mem_ceiling=1.0,
            mem_floor=1.0,
            percentile_n=0.25,
        ))
        self.assertTrue(response.demotion_only)
