import logging
import time
import unittest

import grpc
import subprocess
import sys
import psycopg2

import smdbrpc_pb2
import smdbrpc_pb2_grpc


def query_table_num_from_names(names, host="localhost"):
    db_url = "postgresql://root@{}:26257?sslmode=disable".format(host)

    conn = psycopg2.connect(db_url, database="tpcc")

    mapping = {}
    with conn.cursor() as cur:
        for table_name in names:

            query = "SELECT '\"{}\"'::regclass::oid;".format(table_name)
            print(query)

            cur.execute(query)
            logging.debug("status message %s", cur.statusmessage)

            rows = cur.fetchall()
            if len(rows) > 1:
                print("fetchall should only have one row")
                sys.exit(-1)

            mapping[table_name] = rows[0][0]

        conn.commit()

    return mapping


def call(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT):
    """
    Calls a command in the shell.

    :param cmd: (str)
    :param stdout: set by default to subprocess.PIPE (which is standard stream)
    :param stderr: set by default subprocess.STDOUT (combines with stdout)
    :return: if successful, stdout stream of command.
    """
    print(cmd)
    p = subprocess.run(
        cmd, stdout=stdout, stderr=stderr, shell=True, check=True,
        universal_newlines=True
    )
    return p.stdout


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

    def test_query_num_from_name(self):
        tableNames = ["warehouse", "stock", "item", "history", "new_order",
                      "order_line", "district", "customer", "order"]

        mapping = query_table_num_from_names(tableNames)

        self.assertEqual(len(tableNames), len(mapping))
        nameset, numset = {}, {}

        for name, num in mapping.items():

            if name in nameset:
                self.assertTrue(False)
            elif num in numset:
                self.assertTrue(False)

    def test_populate_tables_from_query(self):
        tableNames = ["warehouse", "stock", "item", "history", "new_order",
                      "order_line", "district", "customer", "order"]

        mapping = query_table_num_from_names(tableNames)

        req = smdbrpc_pb2.PopulateCRDBTableNumMappingReq(
            tableNumMappings=[], )
        for tableName, tableNum in mapping.items():
            req.tableNumMappings.append(
                smdbrpc_pb2.TableNumMapping(
                    tableNum=tableNum, tableName=tableName, )
            )

        _ = self.stub.PopulateCRDBTableNumMapping(req)

        checkMappingReq = smdbrpc_pb2.QueryTableMapReq(placeholder=True)
        checkMappingResp = self.stub.TestQueryTableMap(checkMappingReq)

        self.assertEqual(len(mapping), len(checkMappingResp.tableNumMappings))
        for tableNumMapping in checkMappingResp.tableNumMappings:
            name, num = tableNumMapping.tableName, tableNumMapping.tableNum
            self.assertTrue(name in mapping)
            self.assertEqual(mapping[name], num)


if __name__ == "__main__":
    unittest.main()
