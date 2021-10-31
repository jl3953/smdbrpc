import unittest

import grpc

import smdbrpc_pb2_grpc


class MyTestCase(unittest.TestCase):
    def setUp(self):
        self.channel = grpc.insecure_channel("localhost:50055")
        self.stub = smdbrpc_pb2_grpc.HotshardGatewayStub(self.channel)

    def tearDown(self):
        self.channel.close()

    def test_(self):
        self.assertEqual(True, False)  # add assertion here


if __name__ == '__main__':
    unittest.main()
