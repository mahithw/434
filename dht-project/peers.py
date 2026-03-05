# peer.py
# DHT Peer Process - CSE 434 Group 59
#
# Usage: python3 peer.py <manager-IPv4> <manager-port>
# Example: python3 peer.py 127.0.0.1 29500
#
# The peer reads commands from stdin and communicates with the manager
# over UDP. It also listens on its own p-port for peer-to-peer messages.
#
# Milestone commands supported:
#   register   <peer-name> <IPv4> <m-port> <p-port>
#   setup-dht  <peer-name> <n> <YYYY>
#
# Peer-to-peer messages handled:
#   set-id   (received by non-leader peers during DHT setup)
#   store    (routes storm records around the ring)

import socket
import sys
import os
import threading

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

# ─────────────────────────────────────────────
# Peer State (global, shared between threads)
# ─────────────────────────────────────────────
my_name     = None   # this peer's registered name
my_ip       = None   # this peer's IPv4 address
my_m_port   = None   # manager-communication port
my_p_port   = None   # peer-to-peer port
my_id       = None   # ring identifier (0..n-1), set during setup
ring_size   = None   # n, set during setup

# right neighbour on the ring: (name, ip, p_port)
right_neighbour = None

# all peers in the ring as list of (name, ip, p_port), indexed by ring id
ring_peers = []

# local hash table: list of size `table_size`, each slot is a record list or None
local_hash_table = []
table_size       = 0

# sockets
m_sock  = None   # socket for talking to the manager
p_sock  = None   # socket for peer-to-peer communication

# manager address
manager_addr = None

# event to signal that set-id has been received (used by listener thread)
set_id_event = threading.Event()

# lock for hash table writes
ht_lock = threading.Lock()

# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────
def log(msg):
    print(f"[{my_name or 'PEER'}] {msg}", flush=True)

# ─────────────────────────────────────────────
# Send a message to the manager and wait for response
# ─────────────────────────────────────────────
def send_to_manager(msg: str) -> str:
    log(f"  --> Manager: '{msg}'")
    m_sock.sendto(encode(msg), manager_addr)
    data, _ = m_sock.recvfrom(BUFFER_SIZE)
    response = decode(data)
    log(f"  <-- Manager: '{response}'")
    return response

# ─────────────────────────────────────────────
# Send a message to a specific peer (by ip, port)
# ─────────────────────────────────────────────
def send_to_peer(ip: str, port: int, msg: str):
    log(f"  --> Peer {ip}:{port}: '{msg[:80]}{'...' if len(msg) > 80 else ''}'")
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

    # Build and open the manager socket bound to m-port
    m_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    m_sock.bind(('', m_port))

    # Build and open the peer socket bound to p-port
    p_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    p_sock.bind(('', p_port))

    # Send register message to manager
    msg = build_message(CMD_REGISTER, name, ip, m_port, p_port)
    response = send_to_manager(msg)

    if response == SUCCESS:
        my_name   = name
        my_ip     = ip
        my_m_port = m_port
        my_p_port = p_port
        log(f"Registered successfully as '{name}'")

        # Start the peer listener thread now that p_sock is open
        t = threading.Thread(target=peer_listener, daemon=True)
        t.start()
    else:
        log(f"Registration FAILED - closing sockets")
        m_sock.close()
        p_sock.close()
        m_sock = None
        p_sock = None

# ─────────────────────────────────────────────
# Command: setup-dht
# Only the leader (the peer that issues this command) runs the full setup.
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

    # Send setup-dht to manager
    msg      = build_message(CMD_SETUP_DHT, name, n, yyyy)
    response = send_to_manager(msg)
    fields   = parse_message(response)

    if fields[0] != SUCCESS:
        log(f"setup-dht FAILED")
        return

    # Parse the n 3-tuples returned by the manager
    # fields = [SUCCESS, "peer0,ip0,port0", "peer1,ip1,port1", ...]
    tuples_raw = fields[1:]   # list of n tuple strings
    ring_peers = [parse_tuple(t) for t in tuples_raw]   # [(name,ip,port), ...]
    ring_size  = len(ring_peers)
    my_id      = 0   # leader is always id=0

    # My right neighbour is ring_peers[1 mod n]
    right_neighbour = ring_peers[1 % ring_size]

    log(f"setup-dht SUCCESS - I am leader (id=0), ring size={ring_size}")
    log(f"  Ring: {ring_peers}")
    log(f"  Right neighbour: {right_neighbour}")

    # ── Step 1: Send set-id to all other peers ──
    for i in range(1, ring_size):
        peer_name, peer_ip, peer_port = ring_peers[i]
        # Build set-id message:
        # "set-id|<id>|<ring_size>|peer0,ip0,port0|peer1,ip1,port1|..."
        tuples_str = [build_tuple(p[0], p[1], p[2]) for p in ring_peers]
        set_id_msg = build_message(CMD_SET_ID, i, ring_size, *tuples_str)
        send_to_peer(peer_ip, peer_port, set_id_msg)
        log(f"  Sent set-id to peer '{peer_name}' (id={i})")

    # ── Step 2: Load the CSV and populate the DHT ──
    # Find the CSV file
    csv_path = find_csv(yyyy)
    if csv_path is None:
        log(f"ERROR: Could not find details-{yyyy}.csv")
        return

    log(f"Loading storm records from {csv_path} ...")
    records = load_storm_records(csv_path)
    num_records = len(records)
    log(f"  Loaded {num_records} records")

    # Compute hash table size = first prime > 2 * num_records
    table_size      = next_prime(2 * num_records)
    local_hash_table = [None] * table_size
    log(f"  Hash table size = {table_size}")

    # Count of records stored at each node (for output)
    record_counts = [0] * ring_size

    for record in records:
        event_id_str = record[0]
        if not event_id_str.isdigit():
            continue   # skip malformed rows
        event_id = int(event_id_str)

        pos, target_id = compute_pos_and_id(event_id, table_size, ring_size)

        if target_id == my_id:
            # Store locally
            with ht_lock:
                local_hash_table[pos] = record
            record_counts[my_id] += 1
        else:
            # Forward via right neighbour (hot-potato around the ring)
            record_str = build_record(record)
            store_msg  = build_message(CMD_STORE, event_id, pos, target_id, record_str)
            rn_name, rn_ip, rn_port = right_neighbour
            send_to_peer(rn_ip, rn_port, store_msg)
            # We don't wait for a response; store messages flow around the ring

    # ── Wait briefly for store messages to propagate ──
    # (In a more robust implementation you'd use ACKs; for milestone this is fine)
    import time
    log(f"Waiting for store messages to propagate around the ring...")
    time.sleep(3)

    # ── Step 3: Print record counts and send dht-complete ──
    # We can only know our own count directly; for demo purposes print what we know
    log(f"\n{'='*50}")
    log(f"DHT Setup Complete - Record Distribution:")
    log(f"  Node {my_id} ('{my_name}'): {record_counts[my_id]} records stored locally")
    log(f"  (Other nodes will print their own counts)")
    log(f"  Total records processed: {num_records}")
    log(f"{'='*50}\n")

    # Send dht-complete to manager
    complete_msg = build_message(CMD_DHT_COMPLETE, my_name)
    response     = send_to_manager(complete_msg)

    if response == SUCCESS:
        log(f"dht-complete acknowledged by manager. DHT is live!")
    else:
        log(f"dht-complete FAILED")

# ─────────────────────────────────────────────
# Peer Listener Thread
# Runs in background, handles incoming p-port messages:
#   set-id  → store my ring id, neighbours, init hash table
#   store   → store or forward a record
# ─────────────────────────────────────────────
def peer_listener():
    global my_id, ring_size, ring_peers, right_neighbour
    global local_hash_table, table_size

    log(f"Peer listener started on p-port {my_p_port}")

    while True:
        try:
            data, addr = p_sock.recvfrom(BUFFER_SIZE)
        except OSError:
            break   # socket closed

        msg    = decode(data)
        fields = parse_message(msg)
        cmd    = fields[0]

        # ── Handle set-id ──
        # Format: "set-id|<id>|<ring_size>|peer0,ip0,port0|peer1,...|..."
        if cmd == CMD_SET_ID:
            my_id     = int(fields[1])
            ring_size = int(fields[2])
            ring_peers = [parse_tuple(t) for t in fields[3:]]

            # My right neighbour
            right_neighbour = ring_peers[(my_id + 1) % ring_size]

            # We don't know table_size yet — it will be sent with the first store
            # For now, initialize a large placeholder; resize on first store
            log(f"set-id received: my_id={my_id}, ring_size={ring_size}")
            log(f"  Right neighbour: {right_neighbour}")
            set_id_event.set()

        # ── Handle store ──
        # Format: "store|<event_id>|<pos>|<target_id>|<record_str>"
        elif cmd == CMD_STORE:
            event_id  = int(fields[1])
            pos       = int(fields[2])
            target_id = int(fields[3])
            record_str = fields[4]

            if my_id is None:
                # Haven't received set-id yet; wait briefly
                import time; time.sleep(0.5)

            if target_id == my_id:
                # This record belongs to me — store it
                record = parse_record(record_str)

                # Initialise hash table if not done yet
                if len(local_hash_table) == 0:
                    # Infer table_size from pos: we need at least pos+1 slots
                    # Use a large safe default; proper size set by leader's table_size
                    needed = max(pos + 1, 10000)
                    with ht_lock:
                        if len(local_hash_table) == 0:
                            local_hash_table.extend([None] * needed)

                # Grow table if needed
                with ht_lock:
                    while len(local_hash_table) <= pos:
                        local_hash_table.append(None)
                    local_hash_table[pos] = record

                log(f"  STORED event_id={event_id} at pos={pos}")

            else:
                # Not for me — forward to right neighbour
                rn_name, rn_ip, rn_port = right_neighbour
                send_to_peer(rn_ip, rn_port, msg)
                log(f"  FORWARDED event_id={event_id} -> '{rn_name}'")

        else:
            log(f"Peer listener: unknown command '{cmd}' from {addr}")

# ─────────────────────────────────────────────
# After setup is complete, print this peer's record count
# Called from the peer_listener after enough time has passed
# ─────────────────────────────────────────────
def print_my_record_count():
    count = sum(1 for slot in local_hash_table if slot is not None)
    log(f"\n{'='*50}")
    log(f"  Node {my_id} ('{my_name}'): {count} records stored")
    log(f"{'='*50}\n")

# ─────────────────────────────────────────────
# Find the CSV data file for a given year
# Looks in ./data/ and current directory
# ─────────────────────────────────────────────
def find_csv(yyyy: str) -> str:
    candidates = [
        f"data/details-{yyyy}.csv",
        f"details-{yyyy}.csv",
        f"data/StormEvents_details-ftp_v1_0_d{yyyy}_c20250520.csv",
        f"StormEvents_details-ftp_v1_0_d{yyyy}_c20250520.csv",
    ]
    # Also search data/ for any file containing the year
    if os.path.isdir("data"):
        for fname in os.listdir("data"):
            if yyyy in fname and fname.endswith(".csv"):
                candidates.append(os.path.join("data", fname))

    for path in candidates:
        if os.path.isfile(path):
            return path
    return None

# ─────────────────────────────────────────────
# Main: read commands from stdin
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
    print(f"[PEER] Available commands: register, setup-dht")
    print(f"[PEER] Type a command and press Enter.")

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
                print("ERROR: You must register first.")
            else:
                cmd_setup_dht(parts)

        elif cmd == "count":
            # Helper command: print how many records this peer has stored
            print_my_record_count()

        elif cmd == "quit" or cmd == "exit":
            print("Exiting.")
            break

        else:
            print(f"Unknown command: '{cmd}'")
            print("Available: register, setup-dht, count, quit")

if __name__ == "__main__":
    main()
