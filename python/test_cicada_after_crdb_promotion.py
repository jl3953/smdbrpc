import time
import unittest

import grpc

import smdbrpc_pb2
import smdbrpc_pb2_grpc


class TestCicadaAfterCRDBPromotion(unittest.TestCase):

    def setUp(self) -> None:
        self.channel = grpc.insecure_channel("localhost:50051")
        self.stub = smdbrpc_pb2_grpc.HotshardGatewayStub(self.channel)

    def test_are_warehouse_keys_promoted(self):
        txnReq = smdbrpc_pb2.TxnReq(
            ops=[],
            timestamp=smdbrpc_pb2.HLCTimestamp(
                walltime=time.time_ns(),
                logicaltime=0,
            )
        )
        table=53
        idx=1
        for w_id in range(3):
            op = smdbrpc_pb2.Op(
                cmd=smdbrpc_pb2.GET,
                table=table,
                index=idx,
                cicada_key_cols=[w_id],
                key=bytes([136+table, 136+idx, 136+w_id, 136]),
                tableName="warehouse",
            )
            txnReq.ops.append(op)

        req = smdbrpc_pb2.BatchSendTxnsReq(txns=[txnReq])
        print(req)
        resp = self.stub.BatchSendTxns(req)

        self.assertEqual(1, len(resp.txnResps))
        txnResp = resp.txnResps[0]
        self.assertTrue(txnResp.is_committed)
        self.assertEqual(3, len(txnResp.responses))
        for i in range(len(txnResp.responses)):
            given_key = txnReq.ops[i].key
            returned_key = txnResp.responses[i].key
            self.assertEqual(given_key, returned_key)


if __name__ == '__main__':
    unittest.main()
