import argparse
import os
from typing import List, Dict
import struct

redis_dict = {}
dir = ""
dbfilename = ""


def convert_to_resp(msg: str) -> str:
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
    args = parser.parse_args()
    global dir, dbfilename

    if args.dir:
        dir = args.dir
    if args.dbfilename:
        dbfilename = args.dbfilename


def parse_rdb() -> dict[bytes, tuple[bytes, int | None]]:
    store: dict[bytes, tuple[bytes, int | None]] = {}
    db_path = dir + "/" + dbfilename
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


# def read_rdb_config() -> Dict:
#     """
#     Reads the RDB configuration from the specified directory and database file

#     Returns:
#         Dict: _description_
#     """
#     rdb_file_loc: str = dir + "/" + dbfilename

#     if os.path.exists(rdb_file_loc):
#         with open(rdb_file_loc, "rb") as rdb_file:
#             rdb_bytes_content = rdb_file.read()
#             rdb_content = str(rdb_bytes_content)
#             print(f"rdb_content is {rdb_content}")
#             print(f"Length of bytes {len(rdb_bytes_content)} and {len(rdb_content)}")
#             if rdb_content:
#                 splited_parts = rdb_content.split("\\")
#                 print(f"{splited_parts}")
#                 resizedb_index = splited_parts.index("xfb")
#                 key_index = resizedb_index + 3
#                 rdb_config = {}
#                 is_key = True
#                 key, value, expiry = "", "", ""
#                 i=key_index
#                 while i < len(splited_parts):
#                     print(f"i is {i} and splited_parts[i] is {splited_parts[i]}")
#                     if splited_parts[i] == "xff":
#                         break
#                     if splited_parts[i] == "xfc":
#                         parse_rdb()
#                         expiry = int.from_bytes(splited_parts[i : i + 8], "little") * 1_000_000
#                         print(f"Expiry is {expiry}")
#                         i += 8
#                         continue
#                     elif is_key:
#                         key = remove_bytes_characters(splited_parts[i])
#                         is_key = False
#                     elif splited_parts[i] == "x00":
#                         is_key = True
#                     else:
#                         value = remove_bytes_characters(splited_parts[i])
#                     if expiry:
#                         rdb_config.update({key: (value, expiry)})
#                     else:
#                         rdb_config.update({key: value})
#                     key, value, expiry = "", "", ""
#                     i+=1
#                 return rdb_config

#         # If RDB file doesn't exist or no args provided, return
#         return {}


# def remove_bytes_characters(string: str) -> str:
#     """
#     Removes the bytes characters from the given string and returns the original string

#     Example:
#         remove_bytes_characters("x1b[32mmykey\x1b[0m") -> "mykey"
#         remove_bytes_characters("t10\x00myvalue\x00") -> "myvalue"

#     Note:
#         The bytes characters are used to represent the color and font attributes in the Redis protocol.

#     Returns:
#         str: The original string without bytes characters
#     """
#     if string.startswith("x"):
#         return string[3:]
#     elif string.startswith("t") or string.startswith("n"):
#         return string[1:]
