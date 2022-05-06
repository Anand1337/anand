#!/usr/bin/env python3
"""
Handshake from the Future

Start a real node and connect to it. Send handshake with different layout that it knows
but with valid future version, and expect to receive HandshakeFailure with current version.
"""
import asyncio
import socket
import sys
import time
import random
import pathlib

import base58
import nacl.signing

sys.path.append(str(pathlib.Path(__file__).resolve().parents[3] / 'lib'))
from cluster import start_cluster
from peer import ED_PREFIX, connect, create_handshake, sign_handshake, BinarySerializer, schema
from messages import network_pb2

nodes = start_cluster(1, 0, 4, None, [], {})


async def main():
    random.seed(0)

    my_key_pair_nacl = nacl.signing.SigningKey.generate()

    conn = await connect(nodes[0].addr())

    handshake = create_handshake(my_key_pair_nacl, nodes[0].node_key.pk, 12345)
    # Use future version
    handshake.handshake.protocol_version = 2**32 - 1

    # First handshake attempt. Should fail with Protocol Version Mismatch
    await conn.send(handshake)
    response = await conn.recv()

    assert response.HasField("handshake_failure"), response.WhichOneof("message_type")
    assert response.handshake_failure.reason == network_pb2.HandshakeFailure.Reason.ProtocolVersionMismatch , network_pb2.HandshakeFailure.Reason.Name(response.handshake_failure.reason)

asyncio.run(main())
