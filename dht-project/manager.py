# manager.py
# DHT Manager Process - CSE 434 Group 59
#
# Usage: python3 manager.py <port>
# Example: python3 manager.py 29500
#
# Handles all manager-side commands:
#   register, setup-dht, dht-complete,
#   query-dht, leave-dht, join-dht, dht-rebuilt,
#   deregister, teardown-dht, teardown-complete

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
dht_active        = False   # True once setup-dht succeeds
dht_complete      = False   # True once dht-complete received
dht_leader        = None    # name of the current DHT leader
waiting_for       = None    # command manager is exclusively waiting for
churn_initiator   = None    # peer that triggered leave-dht or join-dht

# ─────────────────────────────────────────────
# Helper: log
# ─────────────────────────────────────────────
def log(msg):
    print(f"[MANAGER] {msg}", flush=True)

# ─────────────────────────────────────────────
# Handler: register
# Format:   "register|peer-name|IPv4|m-port|p-port"
# Response: "SUCCESS" or "FAILURE"
# ─────────────────────────────────────────────
def handle_register(fields, addr, sock):
    if len(fields) != 5:
        log(f"register: bad field count from {addr}")
        sock.sendto(encode(FAILURE), addr)
        return

    _, name, ip, m_port_str, p_port_str = fields
    m_port = int(m_port_str)
    p_port = int(p_port_str)

    # Duplicate name check
    if name in peers:
        log(f"register: FAILURE - duplicate name '{name}'")
        sock.sendto(encode(FAILURE), addr)
        return

    # Duplicate port check
    for pname, pinfo in peers.items():
        if pinfo['m_port'] == m_port or pinfo['p_port'] == p_port \
           or pinfo['m_port'] == p_port or pinfo['p_port'] == m_port:
            log(f"register: FAILURE - duplicate port for '{name}'")
            sock.sendto(encode(FAILURE), addr)
            return

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
# Format:   "setup-dht|peer-name|n|YYYY"
# Response: "SUCCESS|peer0,ip0,port0|..." or "FAILURE"
# ─────────────────────────────────────────────
def handle_setup_dht(fields, addr, sock):
    global dht_active, dht_leader, waiting_for

    if len(fields) != 4:
        log(f"setup-dht: bad field count from {addr}")
        sock.sendto(encode(FAILURE), addr)
        return

    _, name, n_str, yyyy = fields
    n = int(n_str)

    if name not in peers:
        log(f"setup-dht: FAILURE - '{name}' not registered")
        sock.sendto(encode(FAILURE), addr)
        return
    if n < 3:
        log(f"setup-dht: FAILURE - n={n} < 3")
        sock.sendto(encode(FAILURE), addr)
        return
    if dht_active:
        log(f"setup-dht: FAILURE - DHT already exists")
        sock.sendto(encode(FAILURE), addr)
        return

    free_peers = [p for p, info in peers.items() if info['state'] == STATE_FREE]
    if len(free_peers) < n:
        log(f"setup-dht: FAILURE - not enough free peers ({len(free_peers)} < {n})")
        sock.sendto(encode(FAILURE), addr)
        return

    peers[name]['state'] = STATE_LEADER
    dht_leader = name

    others = [p for p in free_peers if p != name]
    chosen = random.sample(others, n - 1)
    for p in chosen:
        peers[p]['state'] = STATE_IN_DHT

    ring_peers = [name] + chosen
    tuples     = [build_tuple(p, peers[p]['ip'], peers[p]['p_port']) for p in ring_peers]
    response   = build_message(SUCCESS, *tuples)

    dht_active  = True
    waiting_for = CMD_DHT_COMPLETE

    log(f"setup-dht: SUCCESS - leader='{name}', ring={ring_peers}, year={yyyy}")
    log(f"  Waiting exclusively for dht-complete...")
    sock.sendto(encode(response), addr)

# ─────────────────────────────────────────────
# Handler: dht-complete
# Format:   "dht-complete|peer-name"
# Response: "SUCCESS" or "FAILURE"
# ─────────────────────────────────────────────
def handle_dht_complete(fields, addr, sock):
    global dht_complete, waiting_for

    if len(fields) != 2:
        log(f"dht-complete: bad field count from {addr}")
        sock.sendto(encode(FAILURE), addr)
        return

    _, name = fields

    if name != dht_leader:
        log(f"dht-complete: FAILURE - '{name}' is not leader (leader='{dht_leader}')")
        sock.sendto(encode(FAILURE), addr)
        return

    dht_complete = True
    waiting_for  = None

    log(f"dht-complete: SUCCESS - DHT is fully built. Leader='{name}'")
    log(f"  Peer states: { {p: peers[p]['state'] for p in peers} }")
    sock.sendto(encode(SUCCESS), addr)

# ─────────────────────────────────────────────
# Handler: query-dht
# Format:   "query-dht|peer-name"
# Response: "SUCCESS|name,ip,p_port" or "FAILURE"
# ─────────────────────────────────────────────
def handle_query_dht(fields, addr, sock):
    if len(fields) != 2:
        log(f"query-dht: bad field count from {addr}")
        sock.sendto(encode(FAILURE), addr)
        return

    _, name = fields

    if not dht_complete:
        log(f"query-dht: FAILURE - DHT not yet complete")
        sock.sendto(encode(FAILURE), addr)
        return
    if name not in peers:
        log(f"query-dht: FAILURE - '{name}' not registered")
        sock.sendto(encode(FAILURE), addr)
        return
    if peers[name]['state'] != STATE_FREE:
        log(f"query-dht: FAILURE - '{name}' is not Free (state={peers[name]['state']})")
        sock.sendto(encode(FAILURE), addr)
        return

    # Pick any random DHT peer (Leader or InDHT)
    dht_peers = [p for p, info in peers.items()
                 if info['state'] in (STATE_LEADER, STATE_IN_DHT)]
    chosen = random.choice(dht_peers)
    t = build_tuple(chosen, peers[chosen]['ip'], peers[chosen]['p_port'])
    response = build_message(SUCCESS, t)

    log(f"query-dht: SUCCESS - '{name}' directed to '{chosen}'")
    sock.sendto(encode(response), addr)

# ─────────────────────────────────────────────
# Handler: leave-dht
# Format:   "leave-dht|peer-name"
# Response: "SUCCESS" or "FAILURE"
# ─────────────────────────────────────────────
def handle_leave_dht(fields, addr, sock):
    global waiting_for, churn_initiator

    if len(fields) != 2:
        log(f"leave-dht: bad field count from {addr}")
        sock.sendto(encode(FAILURE), addr)
        return

    _, name = fields

    if not dht_complete:
        log(f"leave-dht: FAILURE - DHT not complete")
        sock.sendto(encode(FAILURE), addr)
        return
    if name not in peers:
        log(f"leave-dht: FAILURE - '{name}' not registered")
        sock.sendto(encode(FAILURE), addr)
        return
    if peers[name]['state'] not in (STATE_LEADER, STATE_IN_DHT):
        log(f"leave-dht: FAILURE - '{name}' is not in the DHT")
        sock.sendto(encode(FAILURE), addr)
        return

    churn_initiator = name
    waiting_for     = CMD_DHT_REBUILT

    log(f"leave-dht: SUCCESS - '{name}' leaving. Waiting for dht-rebuilt...")
    sock.sendto(encode(SUCCESS), addr)

# ─────────────────────────────────────────────
# Handler: join-dht
# Format:   "join-dht|peer-name"
# Response: "SUCCESS" or "FAILURE"
# ─────────────────────────────────────────────
def handle_join_dht(fields, addr, sock):
    global waiting_for, churn_initiator

    if len(fields) != 2:
        log(f"join-dht: bad field count from {addr}")
        sock.sendto(encode(FAILURE), addr)
        return

    _, name = fields

    if not dht_complete:
        log(f"join-dht: FAILURE - DHT not complete")
        sock.sendto(encode(FAILURE), addr)
        return
    if name not in peers:
        log(f"join-dht: FAILURE - '{name}' not registered")
        sock.sendto(encode(FAILURE), addr)
        return
    if peers[name]['state'] != STATE_FREE:
        log(f"join-dht: FAILURE - '{name}' is not Free")
        sock.sendto(encode(FAILURE), addr)
        return

    # Tell the joining peer who the current leader is so it can contact the ring
    leader_tuple = build_tuple(dht_leader,
                               peers[dht_leader]['ip'],
                               peers[dht_leader]['p_port'])
    response = build_message(SUCCESS, leader_tuple)

    churn_initiator = name
    waiting_for     = CMD_DHT_REBUILT

    log(f"join-dht: SUCCESS - '{name}' joining. Leader='{dht_leader}'. Waiting for dht-rebuilt...")
    sock.sendto(encode(response), addr)

# ─────────────────────────────────────────────
# Handler: dht-rebuilt
# Format:   "dht-rebuilt|peer-name|new-leader"
# Response: "SUCCESS" or "FAILURE"
# ─────────────────────────────────────────────
def handle_dht_rebuilt(fields, addr, sock):
    global dht_leader, waiting_for, churn_initiator

    if len(fields) != 3:
        log(f"dht-rebuilt: bad field count from {addr}")
        sock.sendto(encode(FAILURE), addr)
        return

    _, name, new_leader = fields

    if name != churn_initiator:
        log(f"dht-rebuilt: FAILURE - expected '{churn_initiator}', got '{name}'")
        sock.sendto(encode(FAILURE), addr)
        return

    # Determine what type of churn just happened by checking the peer's old state
    old_state = peers[name]['state']

    if old_state in (STATE_LEADER, STATE_IN_DHT):
        # It was a leave-dht: the leaving peer becomes Free
        peers[name]['state'] = STATE_FREE
        log(f"dht-rebuilt: '{name}' left DHT, now Free")
    else:
        # It was a join-dht: the joining peer is now InDHT
        peers[name]['state'] = STATE_IN_DHT
        log(f"dht-rebuilt: '{name}' joined DHT, now InDHT")

    # Update leader if it changed
    if new_leader != dht_leader:
        if dht_leader in peers:
            # Old leader becomes InDHT (still in ring, just no longer leading)
            if peers[dht_leader]['state'] == STATE_LEADER:
                peers[dht_leader]['state'] = STATE_IN_DHT
        peers[new_leader]['state'] = STATE_LEADER
        dht_leader = new_leader
        log(f"dht-rebuilt: New leader is '{new_leader}'")

    waiting_for     = None
    churn_initiator = None

    log(f"dht-rebuilt: SUCCESS - DHT rebuilt. Leader='{dht_leader}'")
    log(f"  Peer states: { {p: peers[p]['state'] for p in peers} }")
    sock.sendto(encode(SUCCESS), addr)

# ─────────────────────────────────────────────
# Handler: deregister
# Format:   "deregister|peer-name"
# Response: "SUCCESS" or "FAILURE"
# ─────────────────────────────────────────────
def handle_deregister(fields, addr, sock):
    if len(fields) != 2:
        log(f"deregister: bad field count from {addr}")
        sock.sendto(encode(FAILURE), addr)
        return

    _, name = fields

    if name not in peers:
        log(f"deregister: FAILURE - '{name}' not registered")
        sock.sendto(encode(FAILURE), addr)
        return
    if peers[name]['state'] == STATE_IN_DHT:
        log(f"deregister: FAILURE - '{name}' is InDHT, must leave first")
        sock.sendto(encode(FAILURE), addr)
        return
    if peers[name]['state'] == STATE_LEADER:
        log(f"deregister: FAILURE - '{name}' is Leader, must teardown DHT first")
        sock.sendto(encode(FAILURE), addr)
        return

    del peers[name]
    log(f"deregister: SUCCESS - '{name}' removed")
    log(f"  Remaining peers: {list(peers.keys())}")
    sock.sendto(encode(SUCCESS), addr)

# ─────────────────────────────────────────────
# Handler: teardown-dht
# Format:   "teardown-dht|peer-name"
# Response: "SUCCESS" or "FAILURE"
# ─────────────────────────────────────────────
def handle_teardown_dht(fields, addr, sock):
    global waiting_for

    if len(fields) != 2:
        log(f"teardown-dht: bad field count from {addr}")
        sock.sendto(encode(FAILURE), addr)
        return

    _, name = fields

    if name not in peers:
        log(f"teardown-dht: FAILURE - '{name}' not registered")
        sock.sendto(encode(FAILURE), addr)
        return
    if peers[name]['state'] != STATE_LEADER:
        log(f"teardown-dht: FAILURE - '{name}' is not the leader")
        sock.sendto(encode(FAILURE), addr)
        return

    waiting_for = CMD_TEARDOWN_COMPLETE

    log(f"teardown-dht: SUCCESS - '{name}' tearing down DHT. Waiting for teardown-complete...")
    sock.sendto(encode(SUCCESS), addr)

# ─────────────────────────────────────────────
# Handler: teardown-complete
# Format:   "teardown-complete|peer-name"
# Response: "SUCCESS" or "FAILURE"
# ─────────────────────────────────────────────
def handle_teardown_complete(fields, addr, sock):
    global dht_active, dht_complete, dht_leader, waiting_for

    if len(fields) != 2:
        log(f"teardown-complete: bad field count from {addr}")
        sock.sendto(encode(FAILURE), addr)
        return

    _, name = fields

    if name not in peers or peers[name]['state'] != STATE_LEADER:
        log(f"teardown-complete: FAILURE - '{name}' is not the leader")
        sock.sendto(encode(FAILURE), addr)
        return

    # Set all DHT peers back to Free
    for pname in peers:
        if peers[pname]['state'] in (STATE_LEADER, STATE_IN_DHT):
            peers[pname]['state'] = STATE_FREE

    dht_active   = False
    dht_complete = False
    dht_leader   = None
    waiting_for  = None

    log(f"teardown-complete: SUCCESS - DHT destroyed. All peers now Free.")
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

    dispatch = {
        CMD_REGISTER:          handle_register,
        CMD_SETUP_DHT:         handle_setup_dht,
        CMD_DHT_COMPLETE:      handle_dht_complete,
        CMD_QUERY_DHT:         handle_query_dht,
        CMD_LEAVE_DHT:         handle_leave_dht,
        CMD_JOIN_DHT:          handle_join_dht,
        CMD_DHT_REBUILT:       handle_dht_rebuilt,
        CMD_DEREGISTER:        handle_deregister,
        CMD_TEARDOWN_DHT:      handle_teardown_dht,
        CMD_TEARDOWN_COMPLETE: handle_teardown_complete,
    }

    while True:
        data, addr = sock.recvfrom(BUFFER_SIZE)
        msg    = decode(data)
        log(f"Received from {addr}: '{msg[:120]}'")

        fields = parse_message(msg)
        cmd    = fields[0]

        # Lock: if waiting for a specific command, reject all others
        if waiting_for is not None and cmd != waiting_for:
            log(f"  Rejecting '{cmd}' - currently waiting for '{waiting_for}'")
            sock.sendto(encode(FAILURE), addr)
            continue

        handler = dispatch.get(cmd)
        if handler:
            handler(fields, addr, sock)
        else:
            log(f"  Unknown command: '{cmd}'")
            sock.sendto(encode(FAILURE), addr)

if __name__ == "__main__":
    main()
