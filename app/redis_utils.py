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


def read_rdb_config() -> Dict:
    """
    Reads the RDB configuration from the specified directory and database file

    Returns:
        Dict: _description_
    """
    rdb_file_loc: str = dir + "/" + dbfilename

    if os.path.exists(rdb_file_loc):
        with open(rdb_file_loc, "rb") as rdb_file:
            rdb_content = str(rdb_file.read())
            if rdb_content:
                splited_parts = rdb_content.split("\\")
                resizedb_index = splited_parts.index("xfb")
                key_index = resizedb_index + 4
                rdb_config = {}
                is_key = True
                key, value = "", ""
                for i in range(key_index, len(splited_parts)):
                    if splited_parts[i] == "xff":
                        break
                    if is_key:
                        key = remove_bytes_characters(splited_parts[i])
                        is_key = False
                    elif splited_parts[i] == "x00":
                        is_key = True
                    else:
                        value = remove_bytes_characters(splited_parts[i])
                    rdb_config.update({key: value})
                return rdb_config

        # If RDB file doesn't exist or no args provided, return
        return {}


def remove_bytes_characters(string: str) -> str:
    """
    Removes the bytes characters from the given string and returns the original string

    Example:
        remove_bytes_characters("x1b[32mmykey\x1b[0m") -> "mykey"
        remove_bytes_characters("t10\x00myvalue\x00") -> "myvalue"

    Note:
        The bytes characters are used to represent the color and font attributes in the Redis protocol.

    Returns:
        str: The original string without bytes characters
    """
    if string.startswith("x"):
        return string[3:]
    elif string.startswith("t") or string.startswith("n"):
        return string[1:]
