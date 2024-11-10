from typing import List


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
    resp: str = f"*{len(msg_arr)}" if len(msg_arr) > 1 else ""
    for s in msg_arr:
        resp += f"${len(s)}\r\n{s}\r\n"
    return resp
