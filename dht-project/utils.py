
# utils.py
# Shared utilities for DHT manager and peer processes
# CSE 434 - Group 59 - Port range: 29500-29999

import csv

# ─────────────────────────────────────────────
# Port Configuration (Group 59: 29500–29999)
# ─────────────────────────────────────────────
MANAGER_PORT = 29500

# ─────────────────────────────────────────────
# Message Command Constants
# ─────────────────────────────────────────────
# Peer <-> Manager commands
CMD_REGISTER        = "register"
CMD_SETUP_DHT       = "setup-dht"
CMD_DHT_COMPLETE    = "dht-complete"
CMD_QUERY_DHT       = "query-dht"
CMD_LEAVE_DHT       = "leave-dht"
CMD_JOIN_DHT        = "join-dht"
CMD_DHT_REBUILT     = "dht-rebuilt"
CMD_DEREGISTER      = "deregister"
CMD_TEARDOWN_DHT    = "teardown-dht"
CMD_TEARDOWN_COMPLETE = "teardown-complete"

# Peer <-> Peer commands
CMD_SET_ID          = "set-id"
CMD_STORE           = "store"
CMD_REBUILD_DHT     = "rebuild-dht"
CMD_TEARDOWN        = "teardown"
CMD_RESET_ID        = "reset-id"
CMD_FIND_EVENT      = "find-event"

# ─────────────────────────────────────────────
# Return Codes
# ─────────────────────────────────────────────
SUCCESS = "SUCCESS"
FAILURE = "FAILURE"

# ─────────────────────────────────────────────
# Peer States
# ─────────────────────────────────────────────
STATE_FREE   = "Free"
STATE_LEADER = "Leader"
STATE_IN_DHT = "InDHT"

# ─────────────────────────────────────────────
# Message Delimiter
# ─────────────────────────────────────────────
DELIM = "|"          # field separator within a message
TUPLE_DELIM = ","    # separator within a 3-tuple (name,ip,port)
RECORD_DELIM = ";"   # separator between fields of a storm record

# ─────────────────────────────────────────────
# Application Limits
# ─────────────────────────────────────────────
MAX_PEERS     = 50           # max registered peers supported
MAX_MSG_SIZE  = 65535        # max UDP payload bytes
BUFFER_SIZE   = 65535        # recv buffer size

# ─────────────────────────────────────────────
# Message Encoding / Decoding Helpers
# ─────────────────────────────────────────────

def encode(msg: str) -> bytes:
    """Encode a string message to bytes for UDP transmission."""
    return msg.encode('utf-8')

def decode(data: bytes) -> str:
    """Decode received bytes to a string message."""
    return data.decode('utf-8').strip()

def build_message(*fields) -> str:
    """
    Build a pipe-delimited message string from any number of fields.
    Example: build_message("register", "Alice", "192.168.1.1", "29501", "29502")
             -> "register|Alice|192.168.1.1|29501|29502"
    """
    return DELIM.join(str(f) for f in fields)

def parse_message(msg: str) -> list:
    """
    Parse a pipe-delimited message into a list of fields.
    Example: parse_message("register|Alice|192.168.1.1|29501|29502")
             -> ["register", "Alice", "192.168.1.1", "29501", "29502"]
    """
    return msg.strip().split(DELIM)

def build_tuple(name: str, ip: str, port: int) -> str:
    """
    Build a 3-tuple string: "name,ip,port"
    Used in setup-dht responses and set-id messages.
    """
    return TUPLE_DELIM.join([name, ip, str(port)])

def parse_tuple(t: str) -> tuple:
    """
    Parse a 3-tuple string "name,ip,port" into (name, ip, int(port)).
    """
    parts = t.split(TUPLE_DELIM)
    return (parts[0], parts[1], int(parts[2]))

def build_record(fields: list) -> str:
    """
    Build a semicolon-delimited storm record string from a list of 14 fields.
    """
    return RECORD_DELIM.join(str(f) for f in fields)

def parse_record(record_str: str) -> list:
    """
    Parse a semicolon-delimited storm record string into a list of 14 fields.
    """
    return record_str.split(RECORD_DELIM)

# ─────────────────────────────────────────────
# Prime Number Helper (for hash table sizing)
# ─────────────────────────────────────────────

def is_prime(n: int) -> bool:
    """Return True if n is a prime number."""
    if n < 2:
        return False
    if n == 2:
        return True
    if n % 2 == 0:
        return False
    for i in range(3, int(n**0.5) + 1, 2):
        if n % i == 0:
            return False
    return True

def next_prime(n: int) -> int:
    """Return the first prime number strictly greater than n."""
    candidate = n + 1
    while not is_prime(candidate):
        candidate += 1
    return candidate

# ─────────────────────────────────────────────
# Hash Functions (from §1.2.1 of project spec)
# ─────────────────────────────────────────────

def compute_pos_and_id(event_id: int, table_size: int, ring_size: int) -> tuple:
    """
    Compute (pos, node_id) for a given event_id.
      pos     = event_id mod table_size   (position in local hash table)
      node_id = pos mod ring_size         (which peer stores it)
    Returns (pos, node_id)
    """
    pos     = event_id % table_size
    node_id = pos % ring_size
    return (pos, node_id)

# ─────────────────────────────────────────────
# CSV Parsing Helper
# ─────────────────────────────────────────────

# The 14 fields the project cares about, by CSV column name
RECORD_FIELDS = [
    "EVENT_ID",
    "STATE",
    "YEAR",
    "MONTH_NAME",
    "EVENT_TYPE",
    "CZ_TYPE",
    "CZ_NAME",
    "INJURIES_DIRECT",
    "INJURIES_INDIRECT",
    "DEATHS_DIRECT",
    "DEATHS_INDIRECT",
    "DAMAGE_PROPERTY",
    "DAMAGE_CROPS",
    "TOR_F_SCALE",
]

def load_storm_records(filepath: str) -> list:
    """
    Load storm records from a CSV file.
    Returns a list of lists, each inner list containing the 14 fields
    defined in RECORD_FIELDS (in order). Skips the header row.
    Missing fields default to empty string "".
    """
    records = []
    with open(filepath, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            record = [row.get(field, "").strip() for field in RECORD_FIELDS]
            records.append(record)
    return records


# ─────────────────────────────────────────────
# Pretty Print for Storm Records
# ─────────────────────────────────────────────

def print_record(fields: list):
    """Print a storm record with labeled fields."""
    labels = [
        "Event ID", "State", "Year", "Month", "Event Type",
        "CZ Type", "CZ Name", "Injuries Direct", "Injuries Indirect",
        "Deaths Direct", "Deaths Indirect", "Damage Property",
        "Damage Crops", "Tor F Scale"
    ]
    print("-" * 40)
    for label, value in zip(labels, fields):
        print(f"  {label:<22}: {value}")
    print("-" * 40)
