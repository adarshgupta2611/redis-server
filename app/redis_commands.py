import socket
import time
from typing import List, Tuple
from datetime import datetime, timedelta
from .redis_utils import convert_to_resp
from app import redis_utils


def set_command_helper(
    message_arr: List[str],
    n_args: int,
    client_socket: socket,
    from_master: bool = False,
):
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
    print(
        f"Inside set_command_helper from_master value is {from_master} and length of replicas is {len(redis_utils.replica_sockets)}"
    )

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
        redis_utils.num_write_operations += 1
        for sock_addr, socket in redis_utils.replica_sockets.items():
            print(f"Inside iteration of sockets for command {message_arr}")
            msg: str = " ".join(message_arr)
            resp_msg = redis_utils.convert_to_resp(msg)

            socket.send(resp_msg.encode())
        if not from_master:
            print(f"Sending Ok to client")
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
            resp = client_socket.send(
                "*1\r\n${}\r\n{}\r\n".format(len(keys[0]), keys[0]).encode()
            )
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
        value = rdb_content.get(message_arr[1])
        if value:
            if value[1]:
                curr = time.time_ns()
                if curr > value[1]:
                    client_socket.send("$-1\r\n".encode())
                    return
            resp = convert_to_resp(value[0])
            client_socket.send(resp.encode())
        else:
            client_socket.send("*0\r\n".encode())


def info_command_helper(message_arr: List[str], n_args: int, client_socket: socket):
    if message_arr[1].lower() == "replication":
        data = "role:master"
        if redis_utils.replicaof:
            data = "role:slave"
        data += (
            "master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeebmaster_repl_offset:0"
        )
        resp_msg = redis_utils.convert_to_resp(data)
        client_socket.send(resp_msg.encode())


def wait_command_helper(message_arr: List[str], n_args: int, client_socket: socket):
    num_replicas = int(message_arr[1])
    wait_time = float(message_arr[2]) / 1000
    if num_replicas == 0:
        client_socket.send(f":0\r\n".encode())
        return
    for sock_addr, socket in redis_utils.replica_sockets.items():
        getack_msg = "REPLCONF GETACK *"
        socket.send(redis_utils.convert_to_resp(getack_msg).encode())

    if wait_time:
        time.sleep(wait_time)
    if redis_utils.num_write_operations == 0:
        client_socket.send(f":{len(redis_utils.replica_sockets)}\r\n".encode())
    else:
        ans = 0
        if redis_utils.num_replicas_ack == (
            redis_utils.num_write_operations * len(redis_utils.replica_sockets)
        ):
            ans = len(redis_utils.replica_sockets)
        else:
            ans = redis_utils.num_replicas_ack % len(redis_utils.replica_sockets)
        client_socket.send(f":{ans}\r\n".encode())
    redis_utils.num_replicas_ack = 0
    
def type_command_helper(message_arr: List[str], n_args: int, client_socket: socket):
    key = message_arr[1]
    if redis_utils.redis_streams_dict.get(key):
        client_socket.send("+stream\r\n".encode())
        return
    value = redis_utils.redis_dict.get(key, None)
    if value:
        type_of_value = ""
        if isinstance(value,str):
            type_of_value = "string"
        type_value_resp = redis_utils.convert_to_resp(type_of_value)
        client_socket.send(type_value_resp.encode())
    else:
        client_socket.send("+none\r\n".encode())
        
def xadd_command_helper(message_arr: List[str], n_args: int, client_socket: socket):
    stream_key = message_arr[1]
    stream_key_id = message_arr[2]
    if stream_key_id == "*":
        xadd_auto_gen_time_seqnum(message_arr, n_args, client_socket,stream_key, stream_key_id)
        print(f"Redis Stream is {redis_utils.redis_streams_dict}")
        return
    
    stream_time, stream_seq_num = stream_key_id.split("-")
    if stream_time=="0" and stream_seq_num=="0":
        client_socket.send(b"-ERR The ID specified in XADD must be greater than 0-0\r\n")
        return
    
    if stream_seq_num=="*":
        xadd_auto_gen_seq_num(message_arr, n_args, client_socket,stream_key, stream_key_id, stream_time, stream_seq_num)
        print(f"Redis Stream is {redis_utils.redis_streams_dict}")
        return
    else:
        xadd_default(message_arr, n_args, client_socket,stream_key, stream_key_id, stream_time, stream_seq_num)
        print(f"Redis Stream is {redis_utils.redis_streams_dict}")
        return
        
    
        
def xadd_auto_gen_seq_num(message_arr: List[str], n_args: int, client_socket: socket, stream_key: str, stream_key_id: str, stream_time: str, stream_seq_num: str):
    if len(redis_utils.redis_streams_dict) == 0:
        if stream_time == "0":
            new_stream_key_id = "0-1"
            if redis_utils.redis_streams_dict.get(stream_key):
                redis_utils.redis_streams_dict.get(stream_key).append({"id" : new_stream_key_id,message_arr[3]: message_arr[4]})
            else:
                redis_utils.redis_streams_dict.update({stream_key: [{"id" : new_stream_key_id,message_arr[3]: message_arr[4]}]})
            client_socket.send(redis_utils.convert_to_resp(new_stream_key_id).encode())
            redis_utils.last_stream_id = stream_key_id
            
        else:
            new_stream_key_id = f"{stream_time}-0"
            if redis_utils.redis_streams_dict.get(stream_key):
                redis_utils.redis_streams_dict.get(stream_key).append({"id" : new_stream_key_id,message_arr[3]: message_arr[4]})
            else:
                redis_utils.redis_streams_dict.update({stream_key: [{"id" : new_stream_key_id,message_arr[3]: message_arr[4]}]})
            redis_utils.last_stream_id = new_stream_key_id
            client_socket.send(redis_utils.convert_to_resp(new_stream_key_id).encode())
        return
    else:
        last_stream_id : str= redis_utils.last_stream_id
        last_stream_time, last_stream_seq_num = last_stream_id.split("-")
        if int(stream_time)==int(last_stream_time):
            new_stream_key_id = f"{stream_time}-{int(last_stream_seq_num)+1}"
            if redis_utils.redis_streams_dict.get(stream_key):
                redis_utils.redis_streams_dict.get(stream_key).append({"id" : new_stream_key_id,message_arr[3]: message_arr[4]})
            else:
                redis_utils.redis_streams_dict.update({stream_key: [{"id" : new_stream_key_id,message_arr[3]: message_arr[4]}]})
            redis_utils.last_stream_id = new_stream_key_id
            client_socket.send(redis_utils.convert_to_resp(new_stream_key_id).encode())
        elif int(stream_time)<int(last_stream_time):
            client_socket.send(b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
        else:
            new_stream_key_id = f"{stream_time}-0"
            if redis_utils.redis_streams_dict.get(stream_key):
                redis_utils.redis_streams_dict.get(stream_key).append({"id" : new_stream_key_id,message_arr[3]: message_arr[4]})
            else:
                redis_utils.redis_streams_dict.update({stream_key: [{"id" : new_stream_key_id,message_arr[3]: message_arr[4]}]})
            redis_utils.last_stream_id = new_stream_key_id
            client_socket.send(redis_utils.convert_to_resp(new_stream_key_id).encode())
            
        
            

def xadd_default(message_arr: List[str], n_args: int, client_socket: socket, stream_key: str, stream_key_id: str, stream_time: str, stream_seq_num: str):
    if len(redis_utils.redis_streams_dict) == 0:
        redis_utils.redis_streams_dict.update({stream_key: [{"id" : stream_key_id,message_arr[3]: message_arr[4]}]})
        client_socket.send(redis_utils.convert_to_resp(stream_key_id).encode())
        redis_utils.last_stream_id = stream_key_id
    else:
        last_stream_id : str= redis_utils.last_stream_id
        last_stream_time, last_stream_seq_num = last_stream_id.split("-")
        if int(stream_time)==int(last_stream_time):
            if int(stream_seq_num) <= int(last_stream_seq_num):
                client_socket.send(b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
                return
        elif int(stream_time)<int(last_stream_time):
            client_socket.send(b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
            return
        
        if redis_utils.redis_streams_dict.get(stream_key):
            redis_utils.redis_streams_dict.get(stream_key).append({"id" : stream_key_id,message_arr[3]: message_arr[4]})
        else:
            redis_utils.redis_streams_dict.update({stream_key: [{"id" : stream_key_id,message_arr[3]: message_arr[4]}]})
        client_socket.send(redis_utils.convert_to_resp(stream_key_id).encode())
        redis_utils.last_stream_id = stream_key_id

def xadd_auto_gen_time_seqnum(message_arr: List[str], n_args: int, client_socket: socket, stream_key: str, stream_key_id: str):
    time_now = int(time.time()*1000)
    if len(redis_utils.redis_streams_dict) == 0:
        new_stream_key_id = f"{time_now}-0"
        redis_utils.redis_streams_dict.update({stream_key: [{"id" : new_stream_key_id,message_arr[3]: message_arr[4]}]})
        client_socket.send(redis_utils.convert_to_resp(new_stream_key_id).encode())
        redis_utils.last_stream_id = new_stream_key_id
        return
    else:
        last_stream_id : str = redis_utils.last_stream_id
        last_stream_time, last_stream_seq_num = last_stream_id.split("-")
        if int(time_now)==int(last_stream_time):
            new_stream_key_id = f"{time_now}-{int(last_stream_seq_num) + 1}"
            if redis_utils.redis_streams_dict.get(stream_key):
                redis_utils.redis_streams_dict.get(stream_key).append({"id" : new_stream_key_id,message_arr[3]: message_arr[4]})
            else:
                redis_utils.redis_streams_dict.update({stream_key: [{"id" : new_stream_key_id,message_arr[3]: message_arr[4]}]})
            client_socket.send(redis_utils.convert_to_resp(new_stream_key_id).encode())
            redis_utils.last_stream_id = new_stream_key_id
            return
        else:
            new_stream_key_id = f"{time_now}-0"
            if redis_utils.redis_streams_dict.get(stream_key):
                redis_utils.redis_streams_dict.get(stream_key).append({"id" : new_stream_key_id,message_arr[3]: message_arr[4]})
            else:
                redis_utils.redis_streams_dict.update({stream_key: [{"id" : new_stream_key_id,message_arr[3]: message_arr[4]}]})
            client_socket.send(redis_utils.convert_to_resp(new_stream_key_id).encode())
            redis_utils.last_stream_id = new_stream_key_id
            return
        
def xrange_command_helper(message_arr: List[str], n_args: int, client_socket: socket):
    stream_key = message_arr[1]
    from_id = message_arr[2]
    to_id = message_arr[3]
    stream_list = redis_utils.redis_streams_dict.get(stream_key, None)
    print(f"xrange_command_helper Stream List found is {stream_list}")
    if from_id == "-":
        to_stream_time, to_seq_num = redis_utils.find_time_and_seq(to_id)
        xrange_start_command_helper(message_arr, n_args, client_socket, stream_list,to_stream_time,to_seq_num)
    elif to_id == "+":
        from_stream_time, from_seq_num = redis_utils.find_time_and_seq(from_id)
        xrange_end_command_helper(message_arr, n_args, client_socket, stream_list,from_stream_time, from_seq_num)
    else:
        from_stream_time, from_seq_num = redis_utils.find_time_and_seq(from_id)
        to_stream_time, to_seq_num = redis_utils.find_time_and_seq(to_id)
        xrange_both_command_helper(message_arr, n_args, client_socket, stream_list, from_stream_time,from_seq_num,to_stream_time,to_seq_num)
    
            
def xrange_both_command_helper(message_arr: List[str], n_args: int, client_socket: socket, stream_list, from_stream_time,from_seq_num,to_stream_time,to_seq_num):
    if stream_list:
        valid_list = []
        for item in stream_list:
            stream_id = item.get("id")
            stream_time, stream_seq_num = stream_id.split("-")
            print(f"from_stream_time {from_stream_time} from_seq_num {from_seq_num} stream_time {stream_time} stream_seq_num {stream_seq_num} to_stream_time {to_stream_time} to_seq_num {to_seq_num}")
            if int(stream_time) < int(from_stream_time) or int(stream_time) > int(to_stream_time):
                print(f"inside continue for {item}")
                continue
            
            if int(stream_time) == int(from_stream_time):
                if not from_seq_num or int(stream_seq_num)>=int(from_seq_num):
                    if int(stream_time) == int(to_stream_time):
                        if not to_seq_num or int(stream_seq_num)<=int(to_seq_num):
                            print(f"Inside all conditions for {item}")
                            valid_list.append(item)
            else:
                print(f"Inside else for {item}")    
                valid_list.append(item)
        print(f"xrange_command_helper Valid List found is {valid_list}")
        
        response = f"*{len(valid_list)}\r\n"
        print(f"response before {response}")
        for valid_item in valid_list:
            len_dict = len(valid_item) - 1
            response += f"*{len_dict}\r\n"
            print(f"response after len dict {response}")
            print(f"Type of valid-item is {type(valid_item)}")
            for key,value in valid_item.items():
                if key == "id":
                    print("inside key is id")
                    response += redis_utils.convert_to_resp(valid_item.get("id"))
                    print(f"response after id is {response}")
                else:
                    response += f"*{len_dict*2}\r\n"
                    response += redis_utils.convert_to_resp(key)
                    response += redis_utils.convert_to_resp(value)
                    print(f"response after items is {response}")
                    
        
        print(f"Final Response is {response}")
        client_socket.send(response.encode())
   
        
def xrange_start_command_helper(message_arr: List[str], n_args: int, client_socket: socket, stream_list,to_stream_time,to_seq_num):
    if stream_list:
        valid_list = []
        for item in stream_list:
            stream_id = item.get("id")
            stream_time, stream_seq_num = stream_id.split("-")
            if int(stream_time) > int(to_stream_time):
                continue
            
            if int(stream_time) == int(to_stream_time):
                if not to_seq_num or int(stream_seq_num)<=int(to_seq_num):
                    print(f"Inside all conditions for {item}")
                    valid_list.append(item)
            else:
                print(f"Inside else for {item}")    
                valid_list.append(item)
        print(f"xrange_command_helper Valid List found is {valid_list}")
        
        response = f"*{len(valid_list)}\r\n"
        print(f"response before {response}")
        for valid_item in valid_list:
            len_dict = len(valid_item) - 1
            response += f"*{len_dict}\r\n"
            print(f"response after len dict {response}")
            print(f"Type of valid-item is {type(valid_item)}")
            for key,value in valid_item.items():
                if key == "id":
                    print("inside key is id")
                    response += redis_utils.convert_to_resp(valid_item.get("id"))
                    print(f"response after id is {response}")
                else:
                    response += f"*{len_dict*2}\r\n"
                    response += redis_utils.convert_to_resp(key)
                    response += redis_utils.convert_to_resp(value)
                    print(f"response after items is {response}")
                    
        
        print(f"Final Response is {response}")
        client_socket.send(response.encode())
        
def xrange_end_command_helper(message_arr: List[str], n_args: int, client_socket: socket, stream_list,from_stream_time,from_seq_num):
    if stream_list:
        valid_list = []
        for item in stream_list:
            stream_id = item.get("id")
            stream_time, stream_seq_num = stream_id.split("-")
            if int(stream_time) < int(from_stream_time):
                continue
            
            if int(stream_time) == int(from_stream_time):
                if not from_seq_num or int(stream_seq_num)>=int(from_seq_num):
                    print(f"Inside all conditions for {item}")
                    valid_list.append(item)
            else:
                print(f"Inside else for {item}")    
                valid_list.append(item)
        print(f"xrange_command_helper Valid List found is {valid_list}")
        
        response = f"*{len(valid_list)}\r\n"
        print(f"response before {response}")
        for valid_item in valid_list:
            len_dict = len(valid_item) - 1
            response += f"*{len_dict}\r\n"
            print(f"response after len dict {response}")
            print(f"Type of valid-item is {type(valid_item)}")
            for key,value in valid_item.items():
                if key == "id":
                    print("inside key is id")
                    response += redis_utils.convert_to_resp(valid_item.get("id"))
                    print(f"response after id is {response}")
                else:
                    response += f"*{len_dict*2}\r\n"
                    response += redis_utils.convert_to_resp(key)
                    response += redis_utils.convert_to_resp(value)
                    print(f"response after items is {response}")
                    
        
        print(f"Final Response is {response}")
        client_socket.send(response.encode())

def xread_command_helper(message_arr: List[str], n_args: int, client_socket: socket):
    if message_arr[1].lower() == "streams": 
        len_of_keys = int((len(message_arr) - 2) / 2)
        print(f"len of keys is {len_of_keys}")
        keys_list = message_arr[2:2+len_of_keys]
        from_id_list = message_arr[2+len_of_keys:]
        print(f"Keys_list is {keys_list} and id_list is {from_id_list}")
        stream_list_with_key = []
        for idx,key in enumerate(keys_list):
            from_time,from_seq = redis_utils.find_time_and_seq(from_id_list[idx])
            list_values = redis_utils.redis_streams_dict.get(key)
            valid_values = []
            for item in list_values:
                stream_id = item["id"]
                stream_time, stream_seq_num = redis_utils.find_time_and_seq(stream_id)
                if int(stream_time) < int(from_time):
                    continue
                elif int(stream_time) == int(from_time):
                    if not from_seq or int(stream_seq_num)>int(from_seq):
                        valid_values.append(item)
                else:
                    valid_values.append(item)
            stream_list_with_key.append((key,valid_values))
                    
                        
                
        print(f"stream_list_with_key is {stream_list_with_key}")
        response = redis_utils.convert_xread_streams_to_resp(stream_list_with_key)
        client_socket.send(response.encode())
