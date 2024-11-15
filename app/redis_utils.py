import argparse
from typing import List

redis_dict = {}
dir = ""
dbfilename = ""
port: int = 6379
replicaof = ""
replica_sockets = {}
num_replicas_ack = 0
num_write_operations = 0
replica_ack_offset = 0


def convert_to_resp(msg: str, is_arr: bool = False) -> str:
    """
    Converts a string to RESP (Redis Protocol) format

    Example:
        convert_to_resp("Hello World") -> "*2\r\n$5\r\nHello\r\n$5\r\nWorld\r\n"

    Args:
        msg (str): The input string to be converted

    Returns:
        str: _description_
    """
    msg_arr: List[str] = msg.split(" ")
    resp: str = f"*{len(msg_arr)}\r\n" if len(msg_arr) > 1 else ""
    if resp=="" and is_arr:
        resp = "*1\r\n"
    for s in msg_arr:
        if s:
            resp += f"${len(s)}\r\n{s}\r\n"
        else:
            resp += "$-1\r\n"
    return resp


def redis_args_parse():
    """
    Parses the command line arguments for Redis

    Returns:
        argparse.Namespace: _description_
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", type=str)
    parser.add_argument("--dbfilename", type=str)
    parser.add_argument("--port", type=int)
    parser.add_argument("--replicaof", type=str)
    args = parser.parse_args()
    global dir, dbfilename, port, replicaof
    if args.dir:
        dir = args.dir
    if args.dbfilename:
        dbfilename = args.dbfilename
    if args.port:
        port = int(args.port)
    if args.replicaof:
        replicaof = args.replicaof


def parse_rdb() -> dict[bytes, tuple[bytes, int | None]]:
    """
    Parses a Redis RDB file and returns a dictionary of keys and values

    Returns:
        dict[bytes, tuple[bytes, int | None]]: _description_
    """
    store: dict[bytes, tuple[bytes, int | None]] = {}
    db_path: str = dir + "/" + dbfilename
    try:
        with open(db_path, mode="rb") as db:
            data = db.read()
    except Exception as e:
        print(f"Unable to open file {db_path}: {e}")
        return store
    if data[0:5] != b"REDIS":
        print("Incorrect RDB format")
    pos = 5 + 4
    while pos < len(data):
        op = data[pos]
        pos += 1
        if op == 0xFA:
            key, pos = parse_db_string(data, pos)
            val, pos = parse_db_string(data, pos)
        elif op == 0xFE:
            db_num, pos = parse_db_len(data, pos)
        elif op == 0xFB:
            _, pos = parse_db_len(data, pos)
            _, pos = parse_db_len(data, pos)
        elif op == 0xFD:
            exp = int.from_bytes(data[pos : pos + 4], "little") * 1_000
            exp = int.from_bytes(data[pos : pos + 4], "little") * 1_000_000_000
            pos += 4
            key, val, pos = parse_keyvalue(data, pos)
            store[key.decode()] = (val.decode(), exp)
        elif op == 0xFC:
            exp = int.from_bytes(data[pos : pos + 8], "little")
            exp = int.from_bytes(data[pos : pos + 8], "little") * 1_000_000
            pos += 8
            key, val, pos = parse_keyvalue(data, pos)
            store[key.decode()] = (val.decode(), exp)
        elif op == 0xFF:
            break
        else:
            key, val, pos = parse_keyvalue(data, pos - 1)
            store[key.decode()] = (val.decode(), None)
    return store


def parse_db_len(data: bytes, pos: int) -> tuple[int, int]:
    first = data[pos]
    pos += 1
    start = first >> 6
    if start == 0b00:
        len = first
    elif start == 0b01:
        first = first & 0b00111111
        second = data[pos]
        pos += 1
        len = (first << 8) + second
    elif start == 0b10:
        len = int.from_bytes(data[pos : pos + 4], "little")
        pos += 4
    elif start == 0b11:
        first = first & 0b00111111
        len = 2**first
    else:
        raise Exception(f"Unknown db len type {start} @ {pos}")
    return (len, pos)


def parse_db_string(data: bytes, pos: int) -> tuple[bytes, int]:
    len, pos = parse_db_len(data, pos)
    vstr = data[pos : pos + len]
    pos += len
    return (vstr, pos)


def parse_keyvalue(data: bytes, pos: int) -> tuple[bytes, bytes, int]:
    vtype = data[pos]
    if not (vtype == 0 or 9 < vtype < 14):
        raise Exception(f"Unsupported value type {vtype} at {pos}")
    pos += 1
    key, pos = parse_db_string(data, pos)
    val, pos = parse_db_string(data, pos)
    return (key, val, pos)
    
    
