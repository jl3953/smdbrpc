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

    def test_simple_read(self):
        """
        Tests that reading an un-inserted value comes back empty.
        """
        key = 1994214
        response = self.stub.ContactHotshard(smdbrpc_pb2.HotshardRequest(
            hlctimestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now + 100,
                logicaltime=0,
            ),
            write_keyset=[],
            read_keyset=[key],
        ))
        self.assertTrue(response.is_committed)
        self.assertEqual(0, len(response.read_valueset))

    def test_simple_write(self):
        key = 1994215
        response = self.stub.ContactHotshard(smdbrpc_pb2.HotshardRequest(
            hlctimestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now + 100,
                logicaltime=0,
            ),
            write_keyset=[smdbrpc_pb2.KVPair(key=key, value=key)],
            read_keyset=[],
        ))
        self.assertTrue(response.is_committed)
        response = self.stub.ContactHotshard(smdbrpc_pb2.HotshardRequest(
            hlctimestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now + 200,
                logicaltime=0,
            ),
            write_keyset=[],
            read_keyset=[key],
        ))
        self.assertTrue(response.is_committed)
        self.assertEqual(1, len(response.read_valueset))
        self.assertEqual(key, response.read_valueset[0].value)

    def test_fail_on_write_under_read(self):
        key = 1994216
        response = self.stub.ContactHotshard(smdbrpc_pb2.HotshardRequest(
            hlctimestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now + 100,
                logicaltime=0,
            ),
            write_keyset=[smdbrpc_pb2.KVPair(key=key, value=key)],
            read_keyset=[],
        ))
        self.assertTrue(response.is_committed)
        response = self.stub.ContactHotshard(smdbrpc_pb2.HotshardRequest(
            hlctimestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now + 200,
                logicaltime=0,
            ),
            write_keyset=[],
            read_keyset=[key],
        ))
        self.assertTrue(response.is_committed)
        self.assertEqual(1, len(response.read_valueset))
        self.assertEqual(key, response.read_valueset[0].value)
        response = self.stub.ContactHotshard(smdbrpc_pb2.HotshardRequest(
            hlctimestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now + 150,
                logicaltime=0,
            ),
            write_keyset=[smdbrpc_pb2.KVPair(key=key, value=key + 1)],
            read_keyset=[],
        ))
        self.assertFalse(response.is_committed)
        response = self.stub.ContactHotshard(smdbrpc_pb2.HotshardRequest(
            hlctimestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now + 300,
                logicaltime=0,
            ),
            write_keyset=[],
            read_keyset=[key],
        ))
        self.assertTrue(response.is_committed)
        self.assertTrue(1, len(response.read_valueset))
        self.assertEqual(key, response.read_valueset[0].value)

    def test_succeed_on_non_recent_read(self):
        key = 1994217
        response = self.stub.ContactHotshard(smdbrpc_pb2.HotshardRequest(
            hlctimestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now + 100,
                logicaltime=0,
            ),
            write_keyset=[smdbrpc_pb2.KVPair(key=key, value=key)],
            read_keyset=[],
        ))
        self.assertTrue(response.is_committed)
        response = self.stub.ContactHotshard(smdbrpc_pb2.HotshardRequest(
            hlctimestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now + 200,
                logicaltime=0,
            ),
            write_keyset=[smdbrpc_pb2.KVPair(key=key, value=key + 1)],
            read_keyset=[],
        ))
        self.assertTrue(response.is_committed)
        response = self.stub.ContactHotshard(smdbrpc_pb2.HotshardRequest(
            hlctimestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now + 150,
                logicaltime=0,
            ),
            write_keyset=[],
            read_keyset=[key],
        ))
        self.assertTrue(response.is_committed)
        self.assertTrue(len(response.read_valueset), 1)
        self.assertEqual(key, response.read_valueset[0].value)

    def test_fail_on_write_under_write(self):
        key = 1994218
        response = self.stub.ContactHotshard(smdbrpc_pb2.HotshardRequest(
            hlctimestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now + 100,
                logicaltime=0,
            ),
            write_keyset=[smdbrpc_pb2.KVPair(key=key, value=key)],
            read_keyset=[],
        ))
        self.assertTrue(response.is_committed)
        response = self.stub.ContactHotshard(smdbrpc_pb2.HotshardRequest(
            hlctimestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now + 200,
                logicaltime=0,
            ),
            write_keyset=[smdbrpc_pb2.KVPair(key=key, value=key + 1)],
            read_keyset=[],
        ))
        self.assertTrue(response.is_committed)
        response = self.stub.ContactHotshard(smdbrpc_pb2.HotshardRequest(
            hlctimestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now + 150,
                logicaltime=0,
            ),
            write_keyset=[smdbrpc_pb2.KVPair(key=key, value=key + 10)],
            read_keyset=[],
        ))
        self.assertFalse(response.is_committed)
        response = self.stub.ContactHotshard(smdbrpc_pb2.HotshardRequest(
            hlctimestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now + 300,
                logicaltime=0,
            ),
            write_keyset=[],
            read_keyset=[key],
        ))
        self.assertTrue(response.is_committed)
        self.assertEqual(1, len(response.read_valueset))
        self.assertEqual(key + 1, response.read_valueset[0].value)


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
        response = self.stub.ContactHotshard(smdbrpc_pb2.HotshardRequest(
            hlctimestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now + 100,
                logicaltime=0,
            ),
            write_keyset=[],
            read_keyset=[key1, key2],
        ))
        self.assertTrue(response.is_committed)
        self.assertEqual(0, len(response.read_valueset))

    def test_simple_write(self):
        key1 = 1994813
        key2 = 20200604
        response = self.stub.ContactHotshard(smdbrpc_pb2.HotshardRequest(
            hlctimestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now + 100,
                logicaltime=0,
            ),
            write_keyset=[smdbrpc_pb2.KVPair(key=key1, value=key1),
                          smdbrpc_pb2.KVPair(key=key2, value=key2),
                          ],
            read_keyset=[],
        ))
        self.assertTrue(response.is_committed)
        response = self.stub.ContactHotshard(smdbrpc_pb2.HotshardRequest(
            hlctimestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now + 200,
                logicaltime=0,
            ),
            write_keyset=[],
            read_keyset=[key1, key2],
        ))
        self.assertTrue(response.is_committed)
        self.assertEqual(2, len(response.read_valueset))
        self.assertEqual(key1, response.read_valueset[0].value)
        self.assertEqual(key2, response.read_valueset[1].value)

    def test_succeed_on_non_recent_read(self):
        key1 = 1994814
        key2 = 20200605
        response = self.stub.ContactHotshard(smdbrpc_pb2.HotshardRequest(
            hlctimestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now + 100,
                logicaltime=0,
            ),
            write_keyset=[smdbrpc_pb2.KVPair(key=key1, value=key1),
                          smdbrpc_pb2.KVPair(key=key2, value=key2),
                          ],
            read_keyset=[],
        ))
        self.assertTrue(response.is_committed)
        response = self.stub.ContactHotshard(smdbrpc_pb2.HotshardRequest(
            hlctimestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now + 200,
                logicaltime=0,
            ),
            write_keyset=[smdbrpc_pb2.KVPair(key=key1, value=key1 + 1),
                          ],
            read_keyset=[],
        ))
        self.assertTrue(response.is_committed)
        response = self.stub.ContactHotshard(smdbrpc_pb2.HotshardRequest(
            hlctimestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now + 150,
                logicaltime=0,
            ),
            write_keyset=[],
            read_keyset=[key1, key2],
        ))
        self.assertTrue(response.is_committed)
        self.assertEqual(len(response.read_valueset), 2)
        self.assertEqual(key1, response.read_valueset[0].value)
        self.assertEqual(key2, response.read_valueset[1].value)

    def test_succeed_on_disjoint_updates(self):
        key1 = 1994815
        key2 = 20200606
        response = self.stub.ContactHotshard(smdbrpc_pb2.HotshardRequest(
            hlctimestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now + 100,
                logicaltime=0,
            ),
            write_keyset=[smdbrpc_pb2.KVPair(key=key1, value=key1),
                          smdbrpc_pb2.KVPair(key=key2, value=key2),
                          ],
            read_keyset=[],
        ))
        self.assertTrue(response.is_committed)
        response = self.stub.ContactHotshard(smdbrpc_pb2.HotshardRequest(
            hlctimestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now + 200,
                logicaltime=0,
            ),
            write_keyset=[smdbrpc_pb2.KVPair(key=key1, value=key1 + 1),
                          ],
            read_keyset=[],
        ))
        self.assertTrue(response.is_committed)
        response = self.stub.ContactHotshard(smdbrpc_pb2.HotshardRequest(
            hlctimestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now + 300,
                logicaltime=0,
            ),
            write_keyset=[],
            read_keyset=[key1, key2],
        ))
        self.assertTrue(response.is_committed)
        self.assertEqual(2, len(response.read_valueset))
        self.assertEqual(key1 + 1, response.read_valueset[0].value)
        self.assertEqual(key2, response.read_valueset[1].value)

    def test_fail_on_write_between_disjoint_updates(self):
        key1 = 1994816
        key2 = 20200607
        response = self.stub.ContactHotshard(smdbrpc_pb2.HotshardRequest(
            hlctimestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now + 100,
                logicaltime=0,
            ),
            write_keyset=[smdbrpc_pb2.KVPair(key=key1, value=key1),
                          smdbrpc_pb2.KVPair(key=key2, value=key2),
                          ],
            read_keyset=[],
        ))
        self.assertTrue(response.is_committed)
        response = self.stub.ContactHotshard(smdbrpc_pb2.HotshardRequest(
            hlctimestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now + 200,
                logicaltime=0,
            ),
            write_keyset=[smdbrpc_pb2.KVPair(key=key1, value=key1 + 1),
                          ],
            read_keyset=[],
        ))
        self.assertTrue(response.is_committed)
        response = self.stub.ContactHotshard(smdbrpc_pb2.HotshardRequest(
            hlctimestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now + 150,
                logicaltime=0,
            ),
            write_keyset=[smdbrpc_pb2.KVPair(key=key1, value=key1 + 10),
                          smdbrpc_pb2.KVPair(key=key2, value=key2 + 10)],
            read_keyset=[],
        ))
        self.assertFalse(response.is_committed)
        response = self.stub.ContactHotshard(smdbrpc_pb2.HotshardRequest(
            hlctimestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=self.now + 300,
                logicaltime=0,
            ),
            write_keyset=[],
            read_keyset=[key1, key2],
        ))
        self.assertTrue(response.is_committed)
        self.assertEqual(2, len(response.read_valueset))
        self.assertEqual(key1 + 1, response.read_valueset[0].value)
        self.assertEqual(key2, response.read_valueset[1].value)


if __name__ == '__main__':
    unittest.main()
