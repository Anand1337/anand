#!/usr/bin/env python3
"""
Handshake

Start a real node and connect to it. Send handshake with wrong genesis and version and
expect receiving HandshakeFailure. Use that information to send valid handshake and
connect to the node.
"""
import asyncio
import socket
import sys
import time
import pathlib

import base58
import nacl.signing

sys.path.append(str(pathlib.Path(__file__).resolve().parents[3] / 'lib'))
from cluster import start_cluster
from peer import ED_PREFIX, connect, create_handshake, sign_handshake
from messages import network_pb2
from messages import schema
from messages.network import EdgeInfo
from serializer import BinarySerializer

nodes = start_cluster(1, 0, 4, None, [], {})


async def main():
    my_key_pair_nacl = nacl.signing.SigningKey.generate()

    conn = await connect(nodes[0].addr())

    handshake = create_handshake(my_key_pair_nacl, nodes[0].node_key.pk, 12345)

    # First handshake attempt. Should fail with Protocol Version Mismatch
    sign_handshake(my_key_pair_nacl, handshake.handshake)
    await conn.send(handshake)
    response = await conn.recv()

    assert response.HasField("handshake_failure"), response.WhichOneof("message_type")
    assert response.handshake_failure.reason == network_pb2.HandshakeFailure.Reason.ProtocolVersionMismatch , network_pb2.HandshakeFailure.Reason.Name(response.handshake_failure.reason)
    handshake.handshake.protocol_version = response.handshake_failure.version

    # Second handshake attempt. Should fail with Genesis Mismatch
    sign_handshake(my_key_pair_nacl, handshake.handshake)
    await conn.send(handshake)
    response = await conn.recv()

    assert response.HasField("handshake_failure"), response.WhichOneof("message_type")
    assert response.handshake_failure.reason == network_pb2.HandshakeFailure.Reason.GenesisMismatch , network_pb2.HandshakeFailure.Reason.Name(response.handshake_failure.reason)
    handshake.handshake.sender_chain_info.genesis_id.CopyFrom(response.handshake_failure.genesis_id)

    # Third handshake attempt. Should succeed
    sign_handshake(my_key_pair_nacl, handshake.handshake)
    await conn.send(handshake)
    response = await conn.recv()

    assert response.HasField("handshake"), response.WhichOneOf("message_type")
    assert response.handshake.sender_chain_info.genesis_id == handshake.handshake.sender_chain_info.genesis_id
    assert BinarySerializer(schema).deserialize(response.handshake.partial_edge_info.borsh,EdgeInfo).nonce == 1
    assert response.handshake.sender_peer_id == handshake.handshake.target_peer_id
    assert response.handshake.target_peer_id == handshake.handshake.sender_peer_id
    assert response.handshake.sender_listen_port == nodes[0].addr()[1]
    assert response.handshake.protocol_version == handshake.handshake.protocol_version


asyncio.run(main())
