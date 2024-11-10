import socket
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
    Handles the KEYS command and retrieves all keys from the Redis dictionary.

    Example:
        keys_get_command_helper(["KEYS", "*"], 2, client_socket)

    Args:
        message_arr (List[str]): _description_
        n_args (int): _description_
        client_socket (socket): _description_
    """
    if message_arr[1].lower() == "*":
        msg = redis_utils.read_rdb_config()
        client_socket.send(f"*1\r\n${len(msg)}\r\n{msg}\r\n".encode())
