import socket
import time
from typing import List, Tuple
from datetime import datetime, timedelta
from .redis_utils import convert_to_resp
from app import redis_utils


def set_command_helper(message_arr: List[str], n_args: int, client_socket: socket):
    """
    Handles the SET command and sets the key-value pair in the Redis dictionary.
    If a time-to-live (TTL) is provided, the key-value pair will expire after the specified time.

    Example:
        set_command_helper(["SET", "mykey", "myvalue"], 3, client_socket)
        set_command_helper(["SET", "mykey", "myvalue", "PX", "1000"], 4, client_socket)

    Args:
        message_arr (List[str]): _description_
        n_args (int): _description_
        client_socket (socket): _description_
    """
    if n_args >= 3:
        index_px = next(
            (i for i, item in enumerate(message_arr) if item.lower() == "px"), -1
        )
        if index_px != -1:
            time_to_expire = datetime.now() + timedelta(
                milliseconds=int(message_arr[index_px + 1])
            )
            redis_utils.redis_dict.update(
                {message_arr[1]: (message_arr[2], time_to_expire)}
            )
        else:
            redis_utils.redis_dict.update({message_arr[1]: message_arr[2]})

        client_socket.send(b"+OK\r\n")
    else:
        client_socket.send(b"-ERR wrong number of arguments for 'SET'\r\n")


def get_command_helper(message_arr: List[str], n_args: int, client_socket: socket):
    """
    Handles the GET command and retrieves the value associated with the given key from the Redis dictionary.

    Example:
        get_command_helper(["GET", "mykey"], 2, client_socket)

    Args:
        message_arr (List[str]): _description_
        n_args (int): _description_
        client_socket (socket): _description_
    """
    now = datetime.now()
    result = redis_utils.redis_dict.get(message_arr[1])
    if not result:
        client_socket.send(b"$-1\r\n")
    elif isinstance(result, str):
        resp = convert_to_resp(redis_utils.redis_dict.get(message_arr[1]))
        client_socket.send(resp.encode())
    elif isinstance(result, Tuple):
        if result[1] < now:
            redis_utils.redis_dict.pop(message_arr[1])
            client_socket.send(b"$-1\r\n")
        else:
            resp = convert_to_resp(result[0])
            client_socket.send(resp.encode())


def config_get_command_helper(
    message_arr: List[str], n_args: int, client_socket: socket
):
    """
    Handles the CONFIG GET command and retrieves the value associated with the given configuration option from Redis.

    Example:
        config_get_command_helper(["CONFIG", "GET", "dir"], 3, client_socket)
        config_get_command_helper(["CONFIG", "GET", "dbfilename"], 3, client_socket)

    Args:
        message_arr (List[str]): _description_
        n_args (int): _description_
        client_socket (socket): _description_
    """
    if message_arr[1].lower() == "get":
        if message_arr[2].lower() == "dir":
            resp = convert_to_resp(f"dir {redis_utils.dir}")
            client_socket.send(resp.encode())
        elif message_arr[2].lower() == "dbfilename":
            resp = convert_to_resp(f"dbfilename {redis_utils.dbfilename}")
            client_socket.send(resp.encode())


def keys_get_command_helper(message_arr: List[str], n_args: int, client_socket: socket):
    """
    Handles the KEYS command and retrieves all keys in the Redis dictionary.

    Args:
        message_arr (List[str]): _description_
        n_args (int): _description_
        client_socket (socket): _description_
    """
    if message_arr[1].lower() == "*":
        rdb_content = redis_utils.parse_rdb()
        keys = list(rdb_content.keys())
        resp = ""
        if len(keys) == 1:
            resp = client_socket.send("*1\r\n${}\r\n{}\r\n".format(len(keys[0]), keys[0]).encode())
        else:
            resp = convert_to_resp(" ".join(keys))
        client_socket.send(resp.encode())


def rdb_get_command_helper(message_arr: List[str], n_args: int, client_socket: socket):
    """
    Handles the RDB GET command and retrieves the value associated with the given key from the Redis RDB file.

    Example:
        rdb_get_command_helper(["RDB", "mykey"], 2, client_socket)

    Args:
        message_arr (List[str]): _description_
        n_args (int): _description_
        client_socket (socket): _description_
    """
    if message_arr[1]:
        rdb_content = redis_utils.parse_rdb()
        print(f"In rdb_get_command_helper rdb_content is {rdb_content}")
        value = rdb_content.get(message_arr[1])
        if value:
            if value[1]:
                curr = time.time_ns()
                if curr>value[1]:
                    client_socket.send("$-1\r\n".encode())
                    return
            resp = convert_to_resp(value[0])
            client_socket.send(resp.encode())
        else:
            client_socket.send("*0\r\n".encode())
