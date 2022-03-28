import time
import unittest

import grpc

import smdbrpc_pb2
import smdbrpc_pb2_grpc


class TestTableMappingCRDB(unittest.TestCase):

    def setUp(self):
        self.channel = grpc.insecure_channel("localhost:50055")
        self.stub = smdbrpc_pb2_grpc.HotshardGatewayStub(self.channel)
        self.now = time.time_ns()

    def tearDown(self):
        self.channel.close()

    def test_map_table_nums(self):
        req = smdbrpc_pb2.PopulateCRDBTableNumMappingReq(
            tableNumMappings=[smdbrpc_pb2.TableNumMapping(
                tableNum=53, tableName="warehouse"
            ), smdbrpc_pb2.TableNumMapping(
                tableNum=54, tableName="district"
            ), smdbrpc_pb2.TableNumMapping(
                tableNum=55, tableName="customer"
            )]
        )

        _ = self.stub.PopulateCRDBTableNumMapping(req)

        checkMappingReq = smdbrpc_pb2.QueryTableMapReq(placeholder=True)
        checkMappingResp = self.stub.TestQueryTableMap(checkMappingReq)

        self.assertEqual(3, len(checkMappingResp.tableNumMappings))
        for tableNumMapping in checkMappingResp.tableNumMappings:
            if ((
                tableNumMapping.tableNum == 53 and tableNumMapping.tableName
                == "warehouse") or (
                tableNumMapping.tableNum == 54 and tableNumMapping.tableName
                == "district") or (
                tableNumMapping.tableNum == 55 and tableNumMapping.tableName
                == "customer")):
                continue
            else:
                self.assertFalse(False)

    if __name__ == "__main__":
        unittest.main()