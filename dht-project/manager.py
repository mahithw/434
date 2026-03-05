# manager.py
# DHT Manager Process - CSE 434 Group 59
#
# Usage: python3 manager.py <port>
# Example: python3 manager.py 29500
#
# The manager runs as an always-on UDP server. It tracks registered peers
# and coordinates DHT setup. For the milestone, it handles:
#   - register
#   - setup-dht
#   - dht-complete

import socket
import sys
import random

from utils import (
    BUFFER_SIZE, SUCCESS, FAILURE,
    STATE_FREE, STATE_LEADER, STATE_IN_DHT,
    CMD_REGISTER, CMD_SETUP_DHT, CMD_DHT_COMPLETE,
    CMD_QUERY_DHT, CMD_LEAVE_DHT, CMD_JOIN_DHT,
    CMD_DHT_REBUILT, CMD_DEREGISTER, CMD_TEARDOWN_DHT,
    CMD_TEARDOWN_COMPLETE,
    build_message, parse_message, build_tuple,
    encode, decode
)

# ─────────────────────────────────────────────
# Manager State
# ─────────────────────────────────────────────

# peers dict: { peer_name -> { 'ip', 'm_port', 'p_port', 'state' } }
peers = {}

# DHT state
dht_active       = False   # True once setup-dht succeeds
dht_complete     = False   # True once dht-complete received
dht_leader       = None    # name of the current DHT leader
waiting_for      = None    # name of command manager is exclusively waiting for

# ─────────────────────────────────────────────
# Helper: log a message with a prefix
# ─────────────────────────────────────────────
def log(msg):
    print(f"[MANAGER] {msg}", flush=True)

# ─────────────────────────────────────────────
# Handler: register
# Message format: "register|peer-name|IPv4|m-port|p-port"
# Response:       "SUCCESS" or "FAILURE"
# ─────────────────────────────────────────────
def handle_register(fields, addr, sock):
    if len(fields) != 5:
        log(f"register: bad field count from {addr}")
        sock.sendto(encode(FAILURE), addr)
        return

    _, name, ip, m_port_str, p_port_str = fields
    m_port = int(m_port_str)
    p_port = int(p_port_str)

    # Check for duplicate name
    if name in peers:
        log(f"register: FAILURE - duplicate name '{name}'")
        sock.sendto(encode(FAILURE), addr)
        return

    # Check for duplicate ports across all registered peers
    for pname, pinfo in peers.items():
        if pinfo['m_port'] == m_port or pinfo['p_port'] == p_port \
           or pinfo['m_port'] == p_port or pinfo['p_port'] == m_port:
            log(f"register: FAILURE - duplicate port for '{name}'")
            sock.sendto(encode(FAILURE), addr)
            return

    # Register the peer
    peers[name] = {
        'ip':     ip,
        'm_port': m_port,
        'p_port': p_port,
        'state':  STATE_FREE
    }
    log(f"register: SUCCESS - '{name}' at {ip}, m-port={m_port}, p-port={p_port}")
    log(f"  Registered peers: {list(peers.keys())}")
    sock.sendto(encode(SUCCESS), addr)

# ─────────────────────────────────────────────
# Handler: setup-dht
# Message format: "setup-dht|peer-name|n|YYYY"
# Response:       "SUCCESS|peer0,ip0,port0|peer1,ip1,port1|..." or "FAILURE"
# ─────────────────────────────────────────────
def handle_setup_dht(fields, addr, sock):
    global dht_active, dht_leader, waiting_for

    if len(fields) != 4:
        log(f"setup-dht: bad field count from {addr}")
        sock.sendto(encode(FAILURE), addr)
        return

    _, name, n_str, yyyy = fields
    n = int(n_str)

    # Validate
    if name not in peers:
        log(f"setup-dht: FAILURE - '{name}' not registered")
        sock.sendto(encode(FAILURE), addr)
        return
    if n < 3:
        log(f"setup-dht: FAILURE - n={n} is less than 3")
        sock.sendto(encode(FAILURE), addr)
        return
    if dht_active:
        log(f"setup-dht: FAILURE - DHT already exists")
        sock.sendto(encode(FAILURE), addr)
        return

    # Count free peers (including the requester)
    free_peers = [p for p, info in peers.items() if info['state'] == STATE_FREE]
    if len(free_peers) < n:
        log(f"setup-dht: FAILURE - not enough free peers ({len(free_peers)} < {n})")
        sock.sendto(encode(FAILURE), addr)
        return

    # Set leader state
    peers[name]['state'] = STATE_LEADER
    dht_leader = name

    # Pick n-1 random free peers (excluding the leader)
    others = [p for p in free_peers if p != name]
    chosen = random.sample(others, n - 1)
    for p in chosen:
        peers[p]['state'] = STATE_IN_DHT

    # Build the list of n 3-tuples: leader first, then chosen peers
    ring_peers = [name] + chosen
    tuples = [build_tuple(p, peers[p]['ip'], peers[p]['p_port']) for p in ring_peers]

    # Build response: "SUCCESS|peer0,ip0,port0|peer1,ip1,port1|..."
    response = build_message(SUCCESS, *tuples)

    # Now only accept dht-complete
    dht_active   = True
    waiting_for  = CMD_DHT_COMPLETE

    log(f"setup-dht: SUCCESS - leader='{name}', ring={ring_peers}, year={yyyy}")
    log(f"  Waiting exclusively for dht-complete...")
    sock.sendto(encode(response), addr)

# ─────────────────────────────────────────────
# Handler: dht-complete
# Message format: "dht-complete|peer-name"
# Response:       "SUCCESS" or "FAILURE"
# ─────────────────────────────────────────────
def handle_dht_complete(fields, addr, sock):
    global dht_complete, waiting_for

    if len(fields) != 2:
        log(f"dht-complete: bad field count from {addr}")
        sock.sendto(encode(FAILURE), addr)
        return

    _, name = fields

    if name != dht_leader:
        log(f"dht-complete: FAILURE - '{name}' is not the leader (leader='{dht_leader}')")
        sock.sendto(encode(FAILURE), addr)
        return

    dht_complete = True
    waiting_for  = None

    log(f"dht-complete: SUCCESS - DHT is fully built. Leader='{name}'")
    log(f"  Peer states: { {p: peers[p]['state'] for p in peers} }")
    sock.sendto(encode(SUCCESS), addr)

# ─────────────────────────────────────────────
# Main server loop
# ─────────────────────────────────────────────
def main():
    if len(sys.argv) != 2:
        print("Usage: python3 manager.py <port>")
        sys.exit(1)

    port = int(sys.argv[1])

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(('', port))

    log(f"Manager started on port {port}")
    log(f"Waiting for peer messages...")

    while True:
        data, addr = sock.recvfrom(BUFFER_SIZE)
        msg = decode(data)
        log(f"Received from {addr}: '{msg}'")

        fields = parse_message(msg)
        cmd    = fields[0]

        # If we are waiting exclusively for a specific command, reject others
        if waiting_for is not None and cmd != waiting_for:
            log(f"  Rejecting '{cmd}' - currently waiting for '{waiting_for}'")
            sock.sendto(encode(FAILURE), addr)
            continue

        # Dispatch to the appropriate handler
        if cmd == CMD_REGISTER:
            handle_register(fields, addr, sock)

        elif cmd == CMD_SETUP_DHT:
            handle_setup_dht(fields, addr, sock)

        elif cmd == CMD_DHT_COMPLETE:
            handle_dht_complete(fields, addr, sock)

        # ── Full project commands (not needed for milestone) ──
        elif cmd in (CMD_QUERY_DHT, CMD_LEAVE_DHT, CMD_JOIN_DHT,
                     CMD_DHT_REBUILT, CMD_DEREGISTER,
                     CMD_TEARDOWN_DHT, CMD_TEARDOWN_COMPLETE):
            log(f"  '{cmd}' not yet implemented (full project)")
            sock.sendto(encode(FAILURE), addr)

        else:
            log(f"  Unknown command: '{cmd}'")
            sock.sendto(encode(FAILURE), addr)

if __name__ == "__main__":
    main()
