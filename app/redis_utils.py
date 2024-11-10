import argparse
from typing import List

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
        resp += f"${len(s)}\r\n{s}\r\n"
    return resp


def redis_args_parse():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dir', type=str)
    parser.add_argument('--dbfilename', type=str)
    args = parser.parse_args()
    global dir, dbfilename

    if args.dir:
        dir = args.dir
    if args.dbfilename:
        dbfilename = args.dbfilename