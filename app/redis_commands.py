import socket
from typing import List, Tuple
from datetime import datetime, timedelta
from .redis_utils import convert_to_resp

redis_dict = {}


def set_command_helper(message_arr: List[str], n_args: int, client_socket: socket):
    if n_args >= 3:
        index_px = next(
            (i for i, item in enumerate(message_arr) if item.lower() == "px"), -1
        )
        if index_px != -1:
            time_to_expire = datetime.now() + timedelta(
                milliseconds=int(message_arr[index_px + 1])
            )
            redis_dict.update({message_arr[1]: (message_arr[2], time_to_expire)})
        else:
            redis_dict.update({message_arr[1]: message_arr[2]})

        client_socket.send(b"+OK\r\n")
    else:
        client_socket.send(b"-ERR wrong number of arguments for 'SET'\r\n")
        
def get_command_helper(message_arr: List[str], n_args: int, client_socket: socket):
    now = datetime.now()
    result = redis_dict.get(message_arr[1])
    if not result:
        client_socket.send(b"$-1\r\n")
    elif isinstance(result,str):
        resp = convert_to_resp(redis_dict.get(message_arr[1]))
        client_socket.send(resp.encode())
    elif isinstance(result,Tuple):
        if result[1] < now:
            redis_dict.pop(message_arr[1])
            client_socket.send(b"$-1\r\n")
        else:
            resp = convert_to_resp(result[0])
            client_socket.send(resp.encode())  
