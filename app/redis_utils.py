import argparse
from typing import List
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


def read_rdb_config() -> str:
    """
    Reads the Redis database from the RDB file and returns the value associated with the "dbfilename" configuration option.

    Args:
        dir (str): The directory where the RDB file is located
        dbfilename (str): The name of the RDB file

    Returns:
        str: The value associated with the "dbfilename" configuration option
    """
    
    rdb_file_loc: str = dir + "/" + dbfilename

    with open(rdb_file_loc, "rb") as f:
        while operand := f.read(1):
            if operand == b"\xfb":
                break
        f.read(3)
        length = struct.unpack("B", f.read(1))[0]
        if length >> 6 == 0b00:
            length = length & 0b00111111
        else:
            length = 0
        val = f.read(length).decode()
        return val
