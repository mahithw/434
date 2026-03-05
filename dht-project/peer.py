# peer.py
# DHT Peer Process - CSE 434 Group 59
#
# Usage: python3 peer.py <manager-IPv4> <manager-port>
# Example: python3 peer.py 127.0.0.1 29500
#
# Milestone commands supported:
#   register   <peer-name> <IPv4> <m-port> <p-port>
#   setup-dht  <peer-name> <n> <YYYY>
#
# Peer-to-peer messages handled:
#   set-id       (received by non-leader peers during DHT setup)
#   store        (routes storm records around the ring)
#   print-count  (leader tells peers to print their record count)

import socket
import sys
import os
import threading
import time

from utils import (
    BUFFER_SIZE, SUCCESS, FAILURE,
    CMD_REGISTER, CMD_SETUP_DHT, CMD_DHT_COMPLETE,
    CMD_SET_ID, CMD_STORE,
    build_message, parse_message, build_tuple, parse_tuple,
    build_record, parse_record,
    encode, decode,
    next_prime, compute_pos_and_id,
    load_storm_records, print_record
)

# Extra peer-to-peer command: leader tells non-leaders to print their count
CMD_PRINT_COUNT = "print-count"

# ─────────────────────────────────────────────
# Peer State (global, shared between threads)
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

set_id_event = threading.Event()
ht_lock      = threading.Lock()

# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────
def log(msg):
    print(f"[{my_name or 'PEER'}] {msg}", flush=True)

# ─────────────────────────────────────────────
# Send to manager, wait for response
# ─────────────────────────────────────────────
def send_to_manager(msg: str) -> str:
    log(f"  --> Manager: '{msg}'")
    m_sock.sendto(encode(msg), manager_addr)
    data, _ = m_sock.recvfrom(BUFFER_SIZE)
    response = decode(data)
    log(f"  <-- Manager: '{response}'")
    return response

# ─────────────────────────────────────────────
# Send to a peer by ip/port (no response expected)
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

    # Ask manager to set up the DHT
    msg      = build_message(CMD_SETUP_DHT, name, n, yyyy)
    response = send_to_manager(msg)
    fields   = parse_message(response)

    if fields[0] != SUCCESS:
        log(f"setup-dht FAILED")
        return

    # Parse n 3-tuples
    ring_peers      = [parse_tuple(t) for t in fields[1:]]
    ring_size       = len(ring_peers)
    my_id           = 0
    right_neighbour = ring_peers[1 % ring_size]

    log(f"setup-dht SUCCESS - I am leader (id=0), ring_size={ring_size}")
    log(f"  Ring: {ring_peers}")
    log(f"  Right neighbour: {right_neighbour}")

    # Step 1: Send set-id to every other peer
    tuples_str = [build_tuple(p[0], p[1], p[2]) for p in ring_peers]
    for i in range(1, ring_size):
        peer_name, peer_ip, peer_port = ring_peers[i]
        set_id_msg = build_message(CMD_SET_ID, i, ring_size, *tuples_str)
        send_to_peer(peer_ip, peer_port, set_id_msg)
        log(f"  Sent set-id (id={i}) to '{peer_name}'")

    # Give peers time to process set-id
    time.sleep(0.5)

    # Step 2: Load CSV and populate the DHT
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

    # Wait for stores to propagate
    log(f"Waiting for store messages to propagate around the ring...")
    time.sleep(3)

    # Step 3: Tell all peers to print their counts
    for i in range(1, ring_size):
        peer_name, peer_ip, peer_port = ring_peers[i]
        send_to_peer(peer_ip, peer_port, build_message(CMD_PRINT_COUNT))

    time.sleep(0.5)

    log(f"\n{'='*55}")
    log(f"  DHT Record Distribution (n={ring_size}, year={yyyy})")
    log(f"  Node {my_id} ('{my_name}' - Leader): {local_count} records")
    log(f"  Total records in dataset : {num_records}")
    log(f"  Hash table size (prime)  : {table_size}")
    log(f"{'='*55}\n")

    # Step 4: Send dht-complete to manager
    complete_msg = build_message(CMD_DHT_COMPLETE, my_name)
    response     = send_to_manager(complete_msg)

    if response == SUCCESS:
        log(f"dht-complete acknowledged by manager. DHT is live!")
    else:
        log(f"dht-complete FAILED")

# ─────────────────────────────────────────────
# Peer Listener Thread
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

        # set-id: "set-id|<id>|<ring_size>|peer0,ip0,port0|..."
        if cmd == CMD_SET_ID:
            my_id           = int(fields[1])
            ring_size       = int(fields[2])
            ring_peers      = [parse_tuple(t) for t in fields[3:]]
            right_neighbour = ring_peers[(my_id + 1) % ring_size]

            log(f"set-id received: my_id={my_id}, ring_size={ring_size}")
            log(f"  Right neighbour: {right_neighbour}")
            set_id_event.set()

        # store: "store|<event_id>|<pos>|<target_id>|<record_str>"
        elif cmd == CMD_STORE:
            event_id   = int(fields[1])
            pos        = int(fields[2])
            target_id  = int(fields[3])
            record_str = fields[4]

            if my_id is None:
                time.sleep(0.5)

            if target_id == my_id:
                record = parse_record(record_str)
                with ht_lock:
                    if len(local_hash_table) == 0:
                        local_hash_table.extend([None] * max(pos + 1, 10000))
                    while len(local_hash_table) <= pos:
                        local_hash_table.append(None)
                    local_hash_table[pos] = record
                log(f"  STORED event_id={event_id} at pos={pos}")
            else:
                rn_name, rn_ip, rn_port = right_neighbour
                send_to_peer(rn_ip, rn_port, msg)
                log(f"  FORWARDED event_id={event_id} -> '{rn_name}'")

        # print-count: leader signals us to print our record count
        elif cmd == CMD_PRINT_COUNT:
            count = sum(1 for slot in local_hash_table if slot is not None)
            log(f"\n{'='*55}")
            log(f"  Node {my_id} ('{my_name}'): {count} records stored")
            log(f"{'='*55}\n")

        else:
            log(f"Peer listener: unknown command '{cmd}' from {addr}")

# ─────────────────────────────────────────────
# Find the CSV data file for a given year
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
    print(f"[PEER] Commands: register, setup-dht, count, quit")

    while True:
        try:
            line = input("> ").strip()
        except EOFError:
            break
        if not line:
            continue

        parts = line.split()
        cmd   = parts[0].lower()

        if cmd == CMD_REGISTER:
            cmd_register(parts)
        elif cmd == CMD_SETUP_DHT:
            if my_name is None:
                print("ERROR: register first.")
            else:
                cmd_setup_dht(parts)
        elif cmd == "count":
            count = sum(1 for slot in local_hash_table if slot is not None)
            log(f"Node {my_id} ('{my_name}'): {count} records stored")
        elif cmd in ("quit", "exit"):
            print("Exiting.")
            break
        else:
            print(f"Unknown command: '{cmd}'")
            print("Commands: register, setup-dht, count, quit")

if __name__ == "__main__":
    main()
