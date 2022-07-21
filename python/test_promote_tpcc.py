import unittest

import grpc

import smdbrpc_pb2
import smdbrpc_pb2_grpc


class PromoteTPCC(unittest.TestCase):

    def setUp(self):
        self.channel = grpc.insecure_channel("localhost:50055")
        self.stub = smdbrpc_pb2_grpc.HotshardGatewayStub(self.channel)

    def test_promote_warehouse_table(self):
        req = smdbrpc_pb2.TestPromoteTPCCTablesReq(
            num_warehouses=3,
            warehouse=True,
            district=False,
            customer=False,
            order=False,
            neworder=False,
            orderline=False,
            stock=False,
            item=False,
            history=False
        )
        _ = self.stub.TestPromoteTPCCTables(req)


if __name__ == '__main__':
    unittest.main()
