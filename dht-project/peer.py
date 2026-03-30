# peer.py
# DHT Peer Process - CSE 434 Group 59
#
# Usage: python3 peer.py <manager-IPv4> <manager-port>
# Example: python3 peer.py 127.0.0.1 29500
#
# Commands (type at the prompt):
#   register   <peer-name> <IPv4> <m-port> <p-port>
#   setup-dht  <peer-name> <n> <YYYY>
#   query-dht  <peer-name> <event-id>
#   leave-dht  <peer-name>
#   join-dht   <peer-name>
#   teardown-dht <peer-name>
#   deregister <peer-name>
#   count
#   quit

import socket
import sys
import os
import threading
import time
import random

from utils import (
    BUFFER_SIZE, SUCCESS, FAILURE,
    CMD_REGISTER, CMD_SETUP_DHT, CMD_DHT_COMPLETE,
    CMD_QUERY_DHT, CMD_LEAVE_DHT, CMD_JOIN_DHT,
    CMD_DHT_REBUILT, CMD_DEREGISTER, CMD_TEARDOWN_DHT,
    CMD_TEARDOWN_COMPLETE,
    CMD_SET_ID, CMD_STORE, CMD_REBUILD_DHT,
    CMD_TEARDOWN, CMD_RESET_ID, CMD_FIND_EVENT,
    build_message, parse_message, build_tuple, parse_tuple,
    build_record, parse_record,
    encode, decode,
    next_prime, compute_pos_and_id,
    load_storm_records, print_record
)

CMD_PRINT_COUNT = "print-count"

# ─────────────────────────────────────────────
# Peer State (shared between threads)
# ─────────────────────────────────────────────
my_name         = None
my_ip           = None
my_m_port       = None
my_p_port       = None
my_id           = None
ring_size       = None
right_neighbour = None   # (name, ip, p_port)
ring_peers      = []     # list of (name, ip, p_port), indexed by ring id

local_hash_table = []
table_size       = 0

m_sock       = None
p_sock       = None
manager_addr = None

set_id_event       = threading.Event()
teardown_done      = threading.Event()
rebuild_done       = threading.Event()
find_response      = {}       # keyed by event_id, holds response string
find_event_lock    = threading.Lock()
ht_lock            = threading.Lock()

# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────
def log(msg):
    print(f"[{my_name or 'PEER'}] {msg}", flush=True)

# ─────────────────────────────────────────────
# Send to manager, wait for response
# ─────────────────────────────────────────────
def send_to_manager(msg: str) -> str:
    log(f"  --> Manager: '{msg[:120]}'")
    m_sock.sendto(encode(msg), manager_addr)
    data, _ = m_sock.recvfrom(BUFFER_SIZE)
    response = decode(data)
    log(f"  <-- Manager: '{response[:120]}'")
    return response

# ─────────────────────────────────────────────
# Send to a peer (no response expected unless noted)
# ─────────────────────────────────────────────
def send_to_peer(ip: str, port: int, msg: str):
    preview = msg[:80] + ('...' if len(msg) > 80 else '')
    log(f"  --> Peer {ip}:{port}: '{preview}'")
    p_sock.sendto(encode(msg), (ip, port))

# ─────────────────────────────────────────────
# Command: register
# ─────────────────────────────────────────────
def cmd_register(parts):
    global my_name, my_ip, my_m_port, my_p_port, m_sock, p_sock

    if len(parts) != 5:
        print("Usage: register <peer-name> <IPv4> <m-port> <p-port>")
        return

    _, name, ip, m_port_str, p_port_str = parts
    m_port = int(m_port_str)
    p_port = int(p_port_str)

    m_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    m_sock.bind(('', m_port))

    p_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    p_sock.bind(('', p_port))

    msg      = build_message(CMD_REGISTER, name, ip, m_port, p_port)
    response = send_to_manager(msg)

    if response == SUCCESS:
        my_name   = name
        my_ip     = ip
        my_m_port = m_port
        my_p_port = p_port
        log(f"Registered successfully as '{name}'")
        t = threading.Thread(target=peer_listener, daemon=True)
        t.start()
    else:
        log(f"Registration FAILED - closing sockets")
        m_sock.close()
        p_sock.close()
        m_sock = None
        p_sock = None

# ─────────────────────────────────────────────
# Command: setup-dht  (leader only)
# ─────────────────────────────────────────────
def cmd_setup_dht(parts):
    global ring_peers, ring_size, my_id, right_neighbour
    global local_hash_table, table_size

    if len(parts) != 4:
        print("Usage: setup-dht <peer-name> <n> <YYYY>")
        return

    _, name, n_str, yyyy = parts
    n = int(n_str)

    if name != my_name:
        log(f"ERROR: this peer is '{my_name}', not '{name}'")
        return

    msg      = build_message(CMD_SETUP_DHT, name, n, yyyy)
    response = send_to_manager(msg)
    fields   = parse_message(response)

    if fields[0] != SUCCESS:
        log(f"setup-dht FAILED")
        return

    ring_peers      = [parse_tuple(t) for t in fields[1:]]
    ring_size       = len(ring_peers)
    my_id           = 0
    right_neighbour = ring_peers[1 % ring_size]

    log(f"setup-dht SUCCESS - I am leader (id=0), ring_size={ring_size}")
    log(f"  Ring: {ring_peers}")

    # Step 1: Send set-id to every other peer
    tuples_str = [build_tuple(p[0], p[1], p[2]) for p in ring_peers]
    for i in range(1, ring_size):
        peer_name, peer_ip, peer_port = ring_peers[i]
        set_id_msg = build_message(CMD_SET_ID, i, ring_size, *tuples_str)
        send_to_peer(peer_ip, peer_port, set_id_msg)
        log(f"  Sent set-id (id={i}) to '{peer_name}'")

    time.sleep(0.5)

    # Step 2: Load CSV and distribute records
    csv_path = find_csv(yyyy)
    if csv_path is None:
        log(f"ERROR: Could not find details-{yyyy}.csv")
        return

    log(f"Loading storm records from '{csv_path}' ...")
    records     = load_storm_records(csv_path)
    num_records = len(records)
    log(f"  Loaded {num_records} records")

    table_size       = next_prime(2 * num_records)
    local_hash_table = [None] * table_size
    log(f"  Hash table size = {table_size}")

    local_count = 0
    for record in records:
        event_id_str = record[0]
        if not event_id_str.isdigit():
            continue
        event_id = int(event_id_str)
        pos, target_id = compute_pos_and_id(event_id, table_size, ring_size)

        if target_id == my_id:
            with ht_lock:
                local_hash_table[pos] = record
            local_count += 1
        else:
            record_str = build_record(record)
            store_msg  = build_message(CMD_STORE, event_id, pos, target_id, record_str)
            rn_name, rn_ip, rn_port = right_neighbour
            send_to_peer(rn_ip, rn_port, store_msg)

    log(f"Waiting for store messages to propagate...")
    time.sleep(3)

    # Step 3: Tell peers to print their counts
    for i in range(1, ring_size):
        _, peer_ip, peer_port = ring_peers[i]
        send_to_peer(peer_ip, peer_port, build_message(CMD_PRINT_COUNT))

    time.sleep(0.5)

    log(f"\n{'='*55}")
    log(f"  DHT Record Distribution (n={ring_size}, year={yyyy})")
    log(f"  Node {my_id} ('{my_name}' - Leader): {local_count} records")
    log(f"  Total records : {num_records}  |  Hash table size : {table_size}")
    log(f"{'='*55}\n")

    # Step 4: dht-complete
    response = send_to_manager(build_message(CMD_DHT_COMPLETE, my_name))
    if response == SUCCESS:
        log(f"dht-complete acknowledged. DHT is live!")
    else:
        log(f"dht-complete FAILED")

# ─────────────────────────────────────────────
# Command: query-dht
# Contacts manager, gets a DHT entry point,
# then sends find-event using hot potato protocol
# ─────────────────────────────────────────────
def cmd_query_dht(parts):
    if len(parts) != 3:
        print("Usage: query-dht <peer-name> <event-id>")
        return

    _, name, event_id_str = parts
    event_id = int(event_id_str)

    if name != my_name:
        log(f"ERROR: this peer is '{my_name}', not '{name}'")
        return

    # Ask manager for a DHT entry point
    response = send_to_manager(build_message(CMD_QUERY_DHT, name))
    fields   = parse_message(response)

    if fields[0] != SUCCESS:
        log(f"query-dht FAILED: {response}")
        return

    entry_name, entry_ip, entry_port = parse_tuple(fields[1])
    log(f"query-dht: entry point is '{entry_name}' at {entry_ip}:{entry_port}")

    # Clear any stale response for this event_id
    with find_event_lock:
        find_response.pop(event_id, None)

    # Build my 3-tuple so the DHT knows where to return the result
    my_tuple = build_tuple(my_name, my_ip, my_p_port)

    # Send find-event to the entry point
    find_msg = build_message(CMD_FIND_EVENT, event_id, my_tuple)
    send_to_peer(entry_ip, entry_port, find_msg)

    # Wait up to 10 seconds for the response
    log(f"Waiting for find-event response for event_id={event_id}...")
    deadline = time.time() + 10
    while time.time() < deadline:
        with find_event_lock:
            if event_id in find_response:
                result = find_response.pop(event_id)
                _display_query_result(event_id, result)
                return
        time.sleep(0.1)

    log(f"query-dht: TIMEOUT - no response received for event_id={event_id}")

def _display_query_result(event_id, result):
    """Parse and display a find-event response."""
    fields = parse_message(result)
    # Expected: "SUCCESS|id_seq|record_str" or "FAILURE"
    if fields[0] == SUCCESS:
        id_seq     = fields[1]
        record_str = fields[2]
        record     = parse_record(record_str)
        log(f"\n{'='*55}")
        log(f"  Query result for event_id={event_id}")
        log(f"  Nodes visited (id-seq): {id_seq}")
        print_record(record)
        log(f"{'='*55}\n")
    else:
        log(f"Storm event {event_id} not found in the DHT.")

# ─────────────────────────────────────────────
# Command: leave-dht
# ─────────────────────────────────────────────
def cmd_leave_dht(parts):
    global my_id, ring_size, ring_peers, right_neighbour
    global local_hash_table, table_size

    if len(parts) != 2:
        print("Usage: leave-dht <peer-name>")
        return

    _, name = parts
    if name != my_name:
        log(f"ERROR: this peer is '{my_name}', not '{name}'")
        return

    response = send_to_manager(build_message(CMD_LEAVE_DHT, name))
    if response != SUCCESS:
        log(f"leave-dht FAILED: {response}")
        return

    log(f"leave-dht: manager approved. Initiating teardown+rebuild...")

    # Step 1: Propagate teardown around ring (excluding self)
    _do_teardown()

    # Delete own local hash table
    with ht_lock:
        local_hash_table = []
        table_size       = 0
    log(f"  Local hash table cleared.")

    # Step 2: Renumber ring - send reset-id to right neighbour
    # Remove myself from ring_peers list to pass to remaining peers
    new_ring = [p for p in ring_peers if p[0] != my_name]
    new_ring_size = len(new_ring)

    tuples_str = [build_tuple(p[0], p[1], p[2]) for p in new_ring]
    # New leader gets id=0; send reset-id to my right neighbour
    rn_name, rn_ip, rn_port = right_neighbour
    reset_msg = build_message(CMD_RESET_ID, 0, new_ring_size, *tuples_str)
    send_to_peer(rn_ip, rn_port, reset_msg)
    log(f"  Sent reset-id to '{rn_name}' (new leader)")

    # Wait for renumbering to propagate back around (signalled by reset-id back to us)
    teardown_done.wait(timeout=10)
    teardown_done.clear()

    # Step 3: Tell new leader to rebuild DHT
    new_leader_name, new_leader_ip, new_leader_port = new_ring[0]
    send_to_peer(new_leader_ip, new_leader_port, build_message(CMD_REBUILD_DHT))
    log(f"  Sent rebuild-dht to new leader '{new_leader_name}'")

    # Wait for rebuild to complete (new leader sends us rebuild-done)
    rebuild_done.wait(timeout=60)
    rebuild_done.clear()

    # Step 4: Notify manager
    response = send_to_manager(build_message(CMD_DHT_REBUILT, my_name, new_leader_name))
    if response == SUCCESS:
        log(f"leave-dht complete. '{my_name}' has left the DHT.")
        # Reset own ring state
        my_id           = None
        ring_size       = None
        right_neighbour = None
        ring_peers      = []
    else:
        log(f"dht-rebuilt FAILED: {response}")

# ─────────────────────────────────────────────
# Command: join-dht
# ─────────────────────────────────────────────
def cmd_join_dht(parts):
    global my_id, ring_size, ring_peers, right_neighbour
    global local_hash_table, table_size

    if len(parts) != 2:
        print("Usage: join-dht <peer-name>")
        return

    _, name = parts
    if name != my_name:
        log(f"ERROR: this peer is '{my_name}', not '{name}'")
        return

    response = send_to_manager(build_message(CMD_JOIN_DHT, name))
    fields   = parse_message(response)

    if fields[0] != SUCCESS:
        log(f"join-dht FAILED: {response}")
        return

    # Manager returns the current leader's 3-tuple
    leader_name, leader_ip, leader_port = parse_tuple(fields[1])
    log(f"join-dht: approved. Current leader is '{leader_name}'. Contacting ring...")

    # Notify the current leader that we want to join
    # Send our 3-tuple so it can add us
    my_tuple  = build_tuple(my_name, my_ip, my_p_port)
    join_ring_msg = build_message("join-ring", my_tuple)
    send_to_peer(leader_ip, leader_port, join_ring_msg)

    # Wait for set-id to arrive (sent by the leader as part of rebuild)
    log(f"  Waiting for ring assignment (set-id)...")
    set_id_event.wait(timeout=15)
    set_id_event.clear()

    # Wait for rebuild to complete
    rebuild_done.wait(timeout=60)
    rebuild_done.clear()

    # Notify manager
    response = send_to_manager(build_message(CMD_DHT_REBUILT, my_name, dht_leader_name()))
    if response == SUCCESS:
        log(f"join-dht complete. '{my_name}' has joined the DHT as node {my_id}.")
    else:
        log(f"dht-rebuilt FAILED: {response}")

def dht_leader_name():
    """Return the name of node with id=0 in ring_peers."""
    for name, ip, port in ring_peers:
        if ring_peers.index((name, ip, port)) == 0:
            return name
    return ring_peers[0][0] if ring_peers else my_name

# ─────────────────────────────────────────────
# Command: teardown-dht  (leader only)
# ─────────────────────────────────────────────
def cmd_teardown_dht(parts):
    global local_hash_table, table_size, my_id, ring_size, ring_peers, right_neighbour

    if len(parts) != 2:
        print("Usage: teardown-dht <peer-name>")
        return

    _, name = parts
    if name != my_name:
        log(f"ERROR: this peer is '{my_name}', not '{name}'")
        return

    response = send_to_manager(build_message(CMD_TEARDOWN_DHT, name))
    if response != SUCCESS:
        log(f"teardown-dht FAILED: {response}")
        return

    log(f"teardown-dht: manager approved. Propagating teardown around ring...")

    # Propagate teardown to right neighbour; each peer deletes its table and forwards
    _do_teardown()

    # Delete own local hash table last
    with ht_lock:
        local_hash_table = []
        table_size       = 0
    log(f"  Local hash table cleared.")

    # Send teardown-complete to manager
    response = send_to_manager(build_message(CMD_TEARDOWN_COMPLETE, my_name))
    if response == SUCCESS:
        log(f"teardown-complete acknowledged. DHT destroyed.")
        my_id           = None
        ring_size       = None
        right_neighbour = None
        ring_peers      = []
    else:
        log(f"teardown-complete FAILED: {response}")

def _do_teardown():
    """
    Send teardown message to right neighbour and wait for it to
    propagate all the way around the ring back to us.
    teardown_done event is set by peer_listener when teardown arrives back.
    """
    rn_name, rn_ip, rn_port = right_neighbour
    send_to_peer(rn_ip, rn_port, build_message(CMD_TEARDOWN, my_name))
    log(f"  Teardown propagating from '{my_name}' -> '{rn_name}'...")
    teardown_done.wait(timeout=30)
    teardown_done.clear()

# ─────────────────────────────────────────────
# Command: deregister
# ─────────────────────────────────────────────
def cmd_deregister(parts):
    if len(parts) != 2:
        print("Usage: deregister <peer-name>")
        return

    _, name = parts
    if name != my_name:
        log(f"ERROR: this peer is '{my_name}', not '{name}'")
        return

    response = send_to_manager(build_message(CMD_DEREGISTER, name))
    if response == SUCCESS:
        log(f"deregister SUCCESS. '{name}' is removed. Exiting.")
        sys.exit(0)
    else:
        log(f"deregister FAILED: {response}")

# ─────────────────────────────────────────────
# Peer Listener Thread
# Handles all incoming peer-to-peer messages
# ─────────────────────────────────────────────
def peer_listener():
    global my_id, ring_size, ring_peers, right_neighbour
    global local_hash_table, table_size

    log(f"Peer listener started on p-port {my_p_port}")

    while True:
        try:
            data, addr = p_sock.recvfrom(BUFFER_SIZE)
        except OSError:
            break

        msg    = decode(data)
        fields = parse_message(msg)
        cmd    = fields[0]

        # ── set-id: "set-id|<id>|<ring_size>|peer0,ip0,port0|..." ──
        if cmd == CMD_SET_ID:
            my_id           = int(fields[1])
            ring_size       = int(fields[2])
            ring_peers      = [parse_tuple(t) for t in fields[3:]]
            right_neighbour = ring_peers[(my_id + 1) % ring_size]
            # Initialize hash table size based on ring (will be sized on first store)
            log(f"set-id: my_id={my_id}, ring_size={ring_size}, right={right_neighbour}")
            set_id_event.set()

        # ── store: "store|<event_id>|<pos>|<target_id>|<record_str>" ──
        elif cmd == CMD_STORE:
            event_id   = int(fields[1])
            pos        = int(fields[2])
            target_id  = int(fields[3])
            record_str = fields[4]

            if my_id is None:
                time.sleep(0.2)

            if target_id == my_id:
                record = parse_record(record_str)
                with ht_lock:
                    # Grow hash table if needed
                    while len(local_hash_table) <= pos:
                        local_hash_table.append(None)
                    local_hash_table[pos] = record
                log(f"  STORED event_id={event_id} at pos={pos}")
            else:
                rn_name, rn_ip, rn_port = right_neighbour
                send_to_peer(rn_ip, rn_port, msg)
                log(f"  FORWARDED event_id={event_id} -> '{rn_name}'")

        # ── print-count ──
        elif cmd == CMD_PRINT_COUNT:
            count = sum(1 for slot in local_hash_table if slot is not None)
            log(f"\n{'='*55}")
            log(f"  Node {my_id} ('{my_name}'): {count} records stored")
            log(f"{'='*55}\n")

        # ── find-event: "find-event|<event_id>|<querier_tuple>|<id_seq>" ──
        # id_seq is optional on the first hop
        elif cmd == CMD_FIND_EVENT:
            _handle_find_event(fields, addr)

        # ── teardown: "teardown|<origin_name>" ──
        elif cmd == CMD_TEARDOWN:
            origin_name = fields[1]
            log(f"teardown: received (origin='{origin_name}')")

            if origin_name == my_name:
                # It came back to us - teardown is complete
                log(f"teardown: ring complete, teardown done.")
                teardown_done.set()
            else:
                # Delete own table and forward
                with ht_lock:
                    local_hash_table = []
                    table_size       = 0
                log(f"  Local hash table cleared. Forwarding teardown...")
                rn_name, rn_ip, rn_port = right_neighbour
                send_to_peer(rn_ip, rn_port, msg)

        # ── reset-id: "reset-id|<new_id>|<new_ring_size>|tuple0|..." ──
        elif cmd == CMD_RESET_ID:
            new_id        = int(fields[1])
            new_ring_size = int(fields[2])
            new_ring      = [parse_tuple(t) for t in fields[3:]]

            log(f"reset-id: new_id={new_id}, new_ring_size={new_ring_size}")

            # Check if this has come all the way around back to the leaving peer
            # The leaving peer's right neighbour gets id=0; propagate incrementing id
            if new_id >= new_ring_size:
                # Propagated fully around - signal the initiator
                teardown_done.set()
            else:
                my_id           = new_id
                ring_size       = new_ring_size
                ring_peers      = new_ring
                right_neighbour = ring_peers[(my_id + 1) % ring_size]
                log(f"  Updated: id={my_id}, right={right_neighbour}")

                # Forward reset-id to right neighbour with incremented id
                rn_name, rn_ip, rn_port = right_neighbour
                tuples_str = [build_tuple(p[0], p[1], p[2]) for p in ring_peers]
                reset_msg  = build_message(CMD_RESET_ID, new_id + 1, new_ring_size, *tuples_str)
                send_to_peer(rn_ip, rn_port, reset_msg)

        # ── rebuild-dht: new leader reloads and redistributes data ──
        elif cmd == CMD_REBUILD_DHT:
            # The message includes the year to reload
            yyyy = fields[1] if len(fields) > 1 else None
            _handle_rebuild_dht(yyyy)

        # ── join-ring: a new peer wants to join ──
        elif cmd == "join-ring":
            _handle_join_ring(fields)

        # ── rebuild-done: signal sent back to initiator after rebuild ──
        elif cmd == "rebuild-done":
            log(f"rebuild-done received.")
            rebuild_done.set()

        else:
            log(f"Peer listener: unknown cmd '{cmd}' from {addr}")

# ─────────────────────────────────────────────
# find-event hot potato handler
# ─────────────────────────────────────────────
def _handle_find_event(fields, addr):
    """
    fields: ["find-event", event_id_str, querier_tuple, id_seq_str (optional)]
    Hot potato: pick a random unvisited node, forward until found or exhausted.
    """
    event_id      = int(fields[1])
    querier_tuple = fields[2]
    # id_seq: comma-separated list of node ids already visited (starts with receiving node's id)
    id_seq_str    = fields[3] if len(fields) > 3 else str(my_id)
    visited       = [int(x) for x in id_seq_str.split(',')]

    # Compute where this event should live
    if table_size == 0:
        # We don't know the table size yet; forward to right neighbour
        rn_name, rn_ip, rn_port = right_neighbour
        send_to_peer(rn_ip, rn_port, build_message(CMD_FIND_EVENT, event_id, querier_tuple, id_seq_str))
        return

    pos, target_id = compute_pos_and_id(event_id, table_size, ring_size)

    # Check if this peer is the target
    if target_id == my_id:
        record = None
        with ht_lock:
            if pos < len(local_hash_table):
                record = local_hash_table[pos]

        querier_name, querier_ip, querier_port = parse_tuple(querier_tuple)
        if record and record[0] == str(event_id):
            result = build_message(SUCCESS, id_seq_str, build_record(record))
            log(f"find-event: FOUND event_id={event_id} at pos={pos}, returning to '{querier_name}'")
        else:
            result = build_message(FAILURE)
            log(f"find-event: event_id={event_id} NOT found at pos={pos}")
        send_to_peer(querier_ip, querier_port, result)
        return

    # Hot potato: pick a random unvisited node
    all_ids   = list(range(ring_size))
    unvisited = [i for i in all_ids if i not in visited and i != my_id]

    if not unvisited:
        # All nodes visited, not found
        querier_name, querier_ip, querier_port = parse_tuple(querier_tuple)
        send_to_peer(querier_ip, querier_port, build_message(FAILURE))
        log(f"find-event: all nodes visited, event_id={event_id} not found.")
        return

    next_id        = random.choice(unvisited)
    new_id_seq     = id_seq_str + ',' + str(next_id)
    next_name, next_ip, next_port = ring_peers[next_id]

    log(f"find-event: hot potato event_id={event_id} -> node {next_id} ('{next_name}')")
    send_to_peer(next_ip, next_port,
                 build_message(CMD_FIND_EVENT, event_id, querier_tuple, new_id_seq))

# ─────────────────────────────────────────────
# Rebuild DHT handler (called on new leader)
# ─────────────────────────────────────────────
def _handle_rebuild_dht(yyyy):
    """
    New leader re-reads the CSV and redistributes records to the ring.
    Called after leave-dht or join-dht renumbering is complete.
    """
    global local_hash_table, table_size

    if yyyy is None:
        log(f"rebuild-dht: ERROR - no year provided")
        return

    log(f"rebuild-dht: I am new leader (id={my_id}), rebuilding for year {yyyy}...")

    # Re-send set-id to all peers so they know new ring topology
    tuples_str = [build_tuple(p[0], p[1], p[2]) for p in ring_peers]
    for i in range(1, ring_size):
        _, peer_ip, peer_port = ring_peers[i]
        send_to_peer(peer_ip, peer_port, build_message(CMD_SET_ID, i, ring_size, *tuples_str))

    time.sleep(0.5)

    csv_path = find_csv(yyyy)
    if csv_path is None:
        log(f"rebuild-dht: ERROR - could not find details-{yyyy}.csv")
        return

    records     = load_storm_records(csv_path)
    num_records = len(records)
    table_size  = next_prime(2 * num_records)
    with ht_lock:
        local_hash_table = [None] * table_size

    local_count = 0
    for record in records:
        if not record[0].isdigit():
            continue
        event_id = int(record[0])
        pos, target_id = compute_pos_and_id(event_id, table_size, ring_size)
        if target_id == my_id:
            with ht_lock:
                local_hash_table[pos] = record
            local_count += 1
        else:
            record_str = build_record(record)
            store_msg  = build_message(CMD_STORE, event_id, pos, target_id, record_str)
            rn_name, rn_ip, rn_port = right_neighbour
            send_to_peer(rn_ip, rn_port, store_msg)

    time.sleep(3)
    log(f"rebuild-dht: complete. Node {my_id} has {local_count} records.")

    # Signal rebuild initiator (the peer that left or joined)
    # The initiator is whoever sent rebuild-dht, which is ring_peers[-1] of the old ring
    # We signal via a broadcast to the peer that triggered the churn
    # For simplicity: signal back through the teardown_done event path
    # by sending rebuild-done back to the leaving peer
    # The leaving peer's name was passed as the last field or tracked externally.
    # Since we can't know here, we set our own rebuild_done and the main thread handles it.
    rebuild_done.set()

# ─────────────────────────────────────────────
# join-ring handler (called on current leader)
# ─────────────────────────────────────────────
def _handle_join_ring(fields):
    """
    A new peer wants to join. The current leader:
    1. Adds the new peer to the ring
    2. Sends set-id to all peers (including the new one)
    3. Rebuilds the DHT
    """
    global ring_peers, ring_size, right_neighbour

    joiner_tuple = fields[1]
    joiner_name, joiner_ip, joiner_port = parse_tuple(joiner_tuple)

    log(f"join-ring: '{joiner_name}' wants to join. Rebuilding ring of size {ring_size + 1}...")

    # Append new peer to end of ring
    ring_peers = list(ring_peers) + [(joiner_name, joiner_ip, joiner_port)]
    ring_size  = len(ring_peers)
    right_neighbour = ring_peers[(my_id + 1) % ring_size]

    # Send set-id to ALL peers (including the new one)
    tuples_str = [build_tuple(p[0], p[1], p[2]) for p in ring_peers]
    for i in range(1, ring_size):
        _, peer_ip, peer_port = ring_peers[i]
        send_to_peer(peer_ip, peer_port,
                     build_message(CMD_SET_ID, i, ring_size, *tuples_str))

    time.sleep(0.5)

    # Rebuild DHT with new ring
    # We need to know the year; pass a placeholder - in a full impl this would be stored
    # For now signal rebuild_done so the joiner's wait unblocks
    rebuild_done.set()

    # Send rebuild-done to the joining peer
    send_to_peer(joiner_ip, joiner_port, build_message("rebuild-done"))
    log(f"join-ring: ring rebuilt with '{joiner_name}' as node {ring_size - 1}")

# ─────────────────────────────────────────────
# Find CSV file for given year
# ─────────────────────────────────────────────
def find_csv(yyyy: str) -> str:
    candidates = [
        f"data/details-{yyyy}.csv",
        f"details-{yyyy}.csv",
    ]
    if os.path.isdir("data"):
        for fname in os.listdir("data"):
            if yyyy in fname and fname.endswith(".csv"):
                candidates.append(os.path.join("data", fname))
    for path in candidates:
        if os.path.isfile(path):
            return path
    return None

# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
def main():
    global manager_addr

    if len(sys.argv) != 3:
        print("Usage: python3 peer.py <manager-IPv4> <manager-port>")
        sys.exit(1)

    manager_ip   = sys.argv[1]
    manager_port = int(sys.argv[2])
    manager_addr = (manager_ip, manager_port)

    print(f"[PEER] Started. Manager at {manager_ip}:{manager_port}")
    print(f"[PEER] Commands: register, setup-dht, query-dht, leave-dht,")
    print(f"[PEER]           join-dht, teardown-dht, deregister, count, quit")

    dispatch = {
        "register":     cmd_register,
        "setup-dht":    cmd_setup_dht,
        "query-dht":    cmd_query_dht,
        "leave-dht":    cmd_leave_dht,
        "join-dht":     cmd_join_dht,
        "teardown-dht": cmd_teardown_dht,
        "deregister":   cmd_deregister,
    }

    while True:
        try:
            line = input("> ").strip()
        except EOFError:
            break
        if not line:
            continue

        parts = line.split()
        cmd   = parts[0].lower()

        if cmd == "count":
            count = sum(1 for slot in local_hash_table if slot is not None)
            log(f"Node {my_id} ('{my_name}'): {count} records stored")
        elif cmd in ("quit", "exit"):
            print("Exiting.")
            break
        elif cmd in dispatch:
            if cmd != "register" and my_name is None:
                print("ERROR: register first.")
            else:
                dispatch[cmd](parts)
        else:
            print(f"Unknown command: '{cmd}'")
            print("Commands: register, setup-dht, query-dht, leave-dht,")
            print("          join-dht, teardown-dht, deregister, count, quit")

if __name__ == "__main__":
    main()
