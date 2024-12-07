import socket
import time
from datetime import datetime, timedelta
from typing import List, Tuple

from app import redis_utils
from .redis_utils import convert_to_resp


def set_command_helper(
        message_arr: List[str],
        n_args: int,
        client_socket: socket,
        addr: str = "",
        from_master: bool = False,
        is_multi_command: bool = False
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
            msg: str = " ".join(message_arr)
            resp_msg = redis_utils.convert_to_resp(msg)

            socket.send(resp_msg.encode())
        if is_multi_command:
            redis_utils.queue_commands_response.get(addr).append("+OK\r\n")
            return
        if not from_master:
            client_socket.send(b"+OK\r\n")

    else:
        client_socket.send(b"-ERR wrong number of arguments for 'SET'\r\n")


def get_command_helper(message_arr: List[str], n_args: int, client_socket: socket, addr: str,
                       is_multi_command: bool = False):
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
        if is_multi_command:
            redis_utils.queue_commands_response.get(addr).append("$-1\r\n")
            return
        client_socket.send(b"$-1\r\n")
    elif isinstance(result, str):
        resp = convert_to_resp(redis_utils.redis_dict.get(message_arr[1]))
        if is_multi_command:
            redis_utils.queue_commands_response.get(addr).append(resp)
            return
        client_socket.send(resp.encode())
    elif isinstance(result, Tuple):
        if result[1] < now:
            redis_utils.redis_dict.pop(message_arr[1])
            if is_multi_command:
                redis_utils.queue_commands_response.get(addr).append("$-1\r\n")
                return
            client_socket.send(b"$-1\r\n")
        else:
            resp = convert_to_resp(result[0])
            if is_multi_command:
                redis_utils.queue_commands_response.get(addr).append(resp)
                return
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
    """
    Handles the INFO command and retrieves information about the Redis server.

    Example:
        info_command_helper(["INFO", "replication"], 2, client_socket)

    Args:
        message_arr (List[str]): The parsed message array.
        n_args (int): The number of arguments in the message array.
        client_socket (socket): The socket representing the client connection.
    """
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
    """
    Handles the WAIT command and waits for a specified number of replicas to acknowledge a write operation.

    Args:
        message_arr (List[str]): The list of command arguments.
        n_args (int): The number of arguments in the command.
        client_socket (socket): The client socket to send responses to.
    """
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
    """
    Handles the TYPE command and returns the type of the value associated with the given key.

    Args:
        message_arr (List[str]): The list of command arguments.
        n_args (int): The number of arguments in the command.
        client_socket (socket): The client socket to send responses to.

    Returns:
        None
    """

    key = message_arr[1]
    if redis_utils.redis_streams_dict.get(key):
        client_socket.send("+stream\r\n".encode())
        return
    value = redis_utils.redis_dict.get(key, None)
    if value:
        type_of_value = ""
        if isinstance(value, str):
            type_of_value = "string"
        type_value_resp = redis_utils.convert_to_resp(type_of_value)
        client_socket.send(type_value_resp.encode())
    else:
        client_socket.send("+none\r\n".encode())


def xadd_command_helper(message_arr: List[str], n_args: int, client_socket: socket):
    """
    Handles the XADD command and appends a new item to the Redis stream with the given key and ID.

    Args:
        message_arr (List[str]): The list of command arguments.
        n_args (int): The number of arguments in the command.
        client_socket (socket): The client socket to send responses to.

    Returns:
        None
    """
    stream_key = message_arr[1]
    stream_key_id = message_arr[2]
    if stream_key_id == "*":
        xadd_auto_gen_time_seqnum(message_arr, n_args, client_socket, stream_key, stream_key_id)
        return
    if redis_utils.wait_until_new_add_stream:
        redis_utils.wait_until_new_add_stream = False
    stream_time, stream_seq_num = stream_key_id.split("-")
    if stream_time == "0" and stream_seq_num == "0":
        client_socket.send(b"-ERR The ID specified in XADD must be greater than 0-0\r\n")
        return

    if stream_seq_num == "*":
        xadd_auto_gen_seq_num(message_arr, n_args, client_socket, stream_key, stream_key_id, stream_time,
                              stream_seq_num)
        return
    else:
        xadd_default(message_arr, n_args, client_socket, stream_key, stream_key_id, stream_time, stream_seq_num)
        return


def xadd_auto_gen_seq_num(message_arr: List[str], n_args: int, client_socket: socket, stream_key: str,
                          stream_key_id: str, stream_time: str, stream_seq_num: str):
    """
    Handles the XADD command with auto-generated sequence numbers for the Redis stream.

    This function is invoked when the sequence number part of the stream ID is specified as "*",
    indicating that the server should auto-generate the sequence number. It updates the Redis stream
    with the new entry and sends the generated stream ID back to the client.

    Args:
        message_arr (List[str]): The list of command arguments.
        n_args (int): The number of arguments in the command.
        client_socket (socket): The client socket to send responses to.
        stream_key (str): The stream key to which the message is being added.
        stream_key_id (str): The stream ID specified in the command.
        stream_time (str): The timestamp part of the stream ID.
        stream_seq_num (str): The sequence number part of the stream ID.

    Returns:
        None
    """
    if len(redis_utils.redis_streams_dict) == 0:
        if stream_time == "0":
            new_stream_key_id = "0-1"
            if redis_utils.redis_streams_dict.get(stream_key):
                redis_utils.redis_streams_dict.get(stream_key).append(
                    {"id": new_stream_key_id, message_arr[3]: message_arr[4]})
            else:
                redis_utils.redis_streams_dict.update(
                    {stream_key: [{"id": new_stream_key_id, message_arr[3]: message_arr[4]}]})
            client_socket.send(redis_utils.convert_to_resp(new_stream_key_id).encode())
            redis_utils.last_stream_id = stream_key_id

        else:
            new_stream_key_id = f"{stream_time}-0"
            if redis_utils.redis_streams_dict.get(stream_key):
                redis_utils.redis_streams_dict.get(stream_key).append(
                    {"id": new_stream_key_id, message_arr[3]: message_arr[4]})
            else:
                redis_utils.redis_streams_dict.update(
                    {stream_key: [{"id": new_stream_key_id, message_arr[3]: message_arr[4]}]})
            redis_utils.last_stream_id = new_stream_key_id
            client_socket.send(redis_utils.convert_to_resp(new_stream_key_id).encode())
        return
    else:
        last_stream_id: str = redis_utils.last_stream_id
        last_stream_time, last_stream_seq_num = last_stream_id.split("-")
        if int(stream_time) == int(last_stream_time):
            new_stream_key_id = f"{stream_time}-{int(last_stream_seq_num) + 1}"
            if redis_utils.redis_streams_dict.get(stream_key):
                redis_utils.redis_streams_dict.get(stream_key).append(
                    {"id": new_stream_key_id, message_arr[3]: message_arr[4]})
            else:
                redis_utils.redis_streams_dict.update(
                    {stream_key: [{"id": new_stream_key_id, message_arr[3]: message_arr[4]}]})
            redis_utils.last_stream_id = new_stream_key_id
            client_socket.send(redis_utils.convert_to_resp(new_stream_key_id).encode())
        elif int(stream_time) < int(last_stream_time):
            client_socket.send(b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
        else:
            new_stream_key_id = f"{stream_time}-0"
            if redis_utils.redis_streams_dict.get(stream_key):
                redis_utils.redis_streams_dict.get(stream_key).append(
                    {"id": new_stream_key_id, message_arr[3]: message_arr[4]})
            else:
                redis_utils.redis_streams_dict.update(
                    {stream_key: [{"id": new_stream_key_id, message_arr[3]: message_arr[4]}]})
            redis_utils.last_stream_id = new_stream_key_id
            client_socket.send(redis_utils.convert_to_resp(new_stream_key_id).encode())


def xadd_default(message_arr: List[str], n_args: int, client_socket: socket, stream_key: str, stream_key_id: str,
                 stream_time: str, stream_seq_num: str):
    """
    Handles the XADD command with auto-generated timestamps and sequence numbers for the Redis stream.

    This function is invoked when the timestamp and sequence number parts of the stream ID are both specified as "*",
    indicating that the server should auto-generate both the timestamp and sequence number. It updates the Redis stream
    with the new entry and sends the generated stream ID back to the client.

    Args:
        message_arr (List[str]): The list of command arguments.
        n_args (int): The number of arguments in the command.
        client_socket (socket): The client socket to send responses to.
        stream_key (str): The stream key to which the message is being added.
        stream_key_id (str): The stream ID specified in the command.
        stream_time (str): The timestamp part of the stream ID.
        stream_seq_num (str): The sequence number part of the stream ID.

    Returns:
        None
    """
    if len(redis_utils.redis_streams_dict) == 0:
        redis_utils.redis_streams_dict.update({stream_key: [{"id": stream_key_id, message_arr[3]: message_arr[4]}]})
        client_socket.send(redis_utils.convert_to_resp(stream_key_id).encode())
        redis_utils.last_stream_id = stream_key_id
    else:
        last_stream_id: str = redis_utils.last_stream_id
        last_stream_time, last_stream_seq_num = last_stream_id.split("-")
        if int(stream_time) == int(last_stream_time):
            if int(stream_seq_num) <= int(last_stream_seq_num):
                client_socket.send(
                    b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
                return
        elif int(stream_time) < int(last_stream_time):
            client_socket.send(b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
            return

        if redis_utils.redis_streams_dict.get(stream_key):
            redis_utils.redis_streams_dict.get(stream_key).append({"id": stream_key_id, message_arr[3]: message_arr[4]})
        else:
            redis_utils.redis_streams_dict.update({stream_key: [{"id": stream_key_id, message_arr[3]: message_arr[4]}]})
        client_socket.send(redis_utils.convert_to_resp(stream_key_id).encode())
        redis_utils.last_stream_id = stream_key_id


def xadd_auto_gen_time_seqnum(message_arr: List[str], n_args: int, client_socket: socket, stream_key: str,
                              stream_key_id: str):
    """
    Handles the XADD command with auto-generated timestamps and sequence numbers for the Redis stream.

    This function is invoked when the timestamp and sequence number parts of the stream ID are both specified as "*",
    indicating that the server should auto-generate both the timestamp and sequence number. It updates the Redis stream
    with the new entry and sends the generated stream ID back to the client.

    Args:
        message_arr (List[str]): The list of command arguments.
        n_args (int): The number of arguments in the command.
        client_socket (socket): The client socket to send responses to.
        stream_key (str): The stream key to which the message is being added.
        stream_key_id (str): The stream ID specified in the command.

    Returns:
        None
    """
    time_now = int(time.time() * 1000)
    if len(redis_utils.redis_streams_dict) == 0:
        new_stream_key_id = f"{time_now}-0"
        redis_utils.redis_streams_dict.update({stream_key: [{"id": new_stream_key_id, message_arr[3]: message_arr[4]}]})
        client_socket.send(redis_utils.convert_to_resp(new_stream_key_id).encode())
        redis_utils.last_stream_id = new_stream_key_id
        return
    else:
        last_stream_id: str = redis_utils.last_stream_id
        last_stream_time, last_stream_seq_num = last_stream_id.split("-")
        if int(time_now) == int(last_stream_time):
            new_stream_key_id = f"{time_now}-{int(last_stream_seq_num) + 1}"
            if redis_utils.redis_streams_dict.get(stream_key):
                redis_utils.redis_streams_dict.get(stream_key).append(
                    {"id": new_stream_key_id, message_arr[3]: message_arr[4]})
            else:
                redis_utils.redis_streams_dict.update(
                    {stream_key: [{"id": new_stream_key_id, message_arr[3]: message_arr[4]}]})
            client_socket.send(redis_utils.convert_to_resp(new_stream_key_id).encode())
            redis_utils.last_stream_id = new_stream_key_id
            return
        else:
            new_stream_key_id = f"{time_now}-0"
            if redis_utils.redis_streams_dict.get(stream_key):
                redis_utils.redis_streams_dict.get(stream_key).append(
                    {"id": new_stream_key_id, message_arr[3]: message_arr[4]})
            else:
                redis_utils.redis_streams_dict.update(
                    {stream_key: [{"id": new_stream_key_id, message_arr[3]: message_arr[4]}]})
            client_socket.send(redis_utils.convert_to_resp(new_stream_key_id).encode())
            redis_utils.last_stream_id = new_stream_key_id
            return


def xrange_command_helper(message_arr: List[str], n_args: int, client_socket: socket):
    """
    Handles the XRANGE command and retrieves a range of entries from the given Redis stream.

    The XRANGE command can be used to retrieve a range of entries from a Redis stream in a variety of ways. The
    command takes two arguments, the first specifying the start of the range and the second specifying the end of the
    range. The start and end arguments can be specified in one of the following ways:

    * If the start and end arguments are both specified as "-", the command will return all entries from the start of the
      stream to the end of the stream.
    * If the start argument is specified as "-" and the end argument is specified as "+", the command will return all
      entries from the start of the stream to the last entry in the stream.
    * If the start argument is specified as "+" and the end argument is specified as "+", the command will return all
      entries from the last entry in the stream to the end of the stream.
    * If the start argument is specified as "+" and the end argument is specified as "-", the command will return all
      entries from the start of the stream to the last entry in the stream.
    * If the start and end arguments are both specified as integers, the command will return all entries in the stream
      with IDs greater than or equal to the start ID and less than or equal to the end ID.

    Args:
        message_arr (List[str]): The list of command arguments.
        n_args (int): The number of arguments in the command.
        client_socket (socket): The client socket to send responses to.

    Returns:
        None
    """
    stream_key = message_arr[1]
    from_id = message_arr[2]
    to_id = message_arr[3]
    stream_list = redis_utils.redis_streams_dict.get(stream_key, None)
    if from_id == "-":
        to_stream_time, to_seq_num = redis_utils.find_time_and_seq(to_id)
        xrange_start_command_helper(message_arr, n_args, client_socket, stream_list, to_stream_time, to_seq_num)
    elif to_id == "+":
        from_stream_time, from_seq_num = redis_utils.find_time_and_seq(from_id)
        xrange_end_command_helper(message_arr, n_args, client_socket, stream_list, from_stream_time, from_seq_num)
    else:
        from_stream_time, from_seq_num = redis_utils.find_time_and_seq(from_id)
        to_stream_time, to_seq_num = redis_utils.find_time_and_seq(to_id)
        xrange_both_command_helper(message_arr, n_args, client_socket, stream_list, from_stream_time, from_seq_num,
                                   to_stream_time, to_seq_num)


def xrange_both_command_helper(message_arr: List[str], n_args: int, client_socket: socket, stream_list,
                               from_stream_time, from_seq_num, to_stream_time, to_seq_num):
    """
    Handles the XRANGE command when both start and end IDs are specified as integers.

    This function retrieves entries from the specified Redis stream that have IDs within the given range. The range is
    defined by the `from_stream_time`, `from_seq_num` (start of the range), and `to_stream_time`, `to_seq_num` (end of the range).
    It collects all valid entries that fall within this range and sends them to the client.

    Args:
        message_arr (List[str]): The list of command arguments.
        n_args (int): The number of arguments in the command.
        client_socket (socket): The client socket to send responses to.
        stream_list: The list of entries in the specified Redis stream.
        from_stream_time: The timestamp part of the start ID.
        from_seq_num: The sequence number part of the start ID.
        to_stream_time: The timestamp part of the end ID.
        to_seq_num: The sequence number part of the end ID.

    Returns:
        None
    """
    if stream_list:
        valid_list = []
        for item in stream_list:
            stream_id = item.get("id")
            stream_time, stream_seq_num = stream_id.split("-")
            if int(stream_time) < int(from_stream_time) or int(stream_time) > int(to_stream_time):
                continue

            if int(stream_time) == int(from_stream_time):
                if not from_seq_num or int(stream_seq_num) >= int(from_seq_num):
                    if int(stream_time) == int(to_stream_time):
                        if not to_seq_num or int(stream_seq_num) <= int(to_seq_num):
                            valid_list.append(item)
            else:
                valid_list.append(item)

        response = f"*{len(valid_list)}\r\n"
        for valid_item in valid_list:
            len_dict = len(valid_item) - 1
            response += f"*{len_dict}\r\n"
            for key, value in valid_item.items():
                if key == "id":
                    response += redis_utils.convert_to_resp(valid_item.get("id"))
                else:
                    response += f"*{len_dict * 2}\r\n"
                    response += redis_utils.convert_to_resp(key)
                    response += redis_utils.convert_to_resp(value)

        client_socket.send(response.encode())
    if stream_list:
        valid_list = []
        for item in stream_list:
            stream_id = item.get("id")
            stream_time, stream_seq_num = stream_id.split("-")
            if int(stream_time) < int(from_stream_time) or int(stream_time) > int(to_stream_time):
                continue

            if int(stream_time) == int(from_stream_time):
                if not from_seq_num or int(stream_seq_num) >= int(from_seq_num):
                    if int(stream_time) == int(to_stream_time):
                        if not to_seq_num or int(stream_seq_num) <= int(to_seq_num):
                            valid_list.append(item)
            else:
                valid_list.append(item)

        response = f"*{len(valid_list)}\r\n"
        for valid_item in valid_list:
            len_dict = len(valid_item) - 1
            response += f"*{len_dict}\r\n"
            for key, value in valid_item.items():
                if key == "id":
                    response += redis_utils.convert_to_resp(valid_item.get("id"))
                else:
                    response += f"*{len_dict * 2}\r\n"
                    response += redis_utils.convert_to_resp(key)
                    response += redis_utils.convert_to_resp(value)

        client_socket.send(response.encode())


def xrange_start_command_helper(message_arr: List[str], n_args: int, client_socket: socket, stream_list, to_stream_time,
                                to_seq_num):
    """
    Handles the XRANGE command and retrieves a range of items from the stream, from the start of the stream to the specified end.

    Args:
        message_arr (List[str]): The list of command arguments.
        n_args (int): The number of arguments in the command.
        client_socket (socket): The client socket to send responses to.
        stream_list: The list of stream items.
        to_stream_time (str): The upper bound of the time range.
        to_seq_num (str): The upper bound of the sequence number range.
    """
    if stream_list:
        valid_list = []
        for item in stream_list:
            stream_id = item.get("id")
            stream_time, stream_seq_num = stream_id.split("-")
            if int(stream_time) > int(to_stream_time):
                continue

            if int(stream_time) == int(to_stream_time):
                if not to_seq_num or int(stream_seq_num) <= int(to_seq_num):
                    valid_list.append(item)
            else:
                valid_list.append(item)

        response = f"*{len(valid_list)}\r\n"
        for valid_item in valid_list:
            len_dict = len(valid_item) - 1
            response += f"*{len_dict}\r\n"
            for key, value in valid_item.items():
                if key == "id":
                    response += redis_utils.convert_to_resp(valid_item.get("id"))
                else:
                    response += f"*{len_dict * 2}\r\n"
                    response += redis_utils.convert_to_resp(key)
                    response += redis_utils.convert_to_resp(value)

        client_socket.send(response.encode())


def xrange_end_command_helper(message_arr: List[str], n_args: int, client_socket: socket, stream_list, from_stream_time,
                              from_seq_num):
    """
    Handles the XRANGE command with a specified end time and sequence number.

    Args:
        message_arr (List[str]): The list of command arguments.
        n_args (int): The number of arguments in the command.
        client_socket (socket): The client socket to send responses to.
        stream_list: The list of stream items.
        from_stream_time (str): The upper bound of the time range.
        from_seq_num (str): The upper bound of the sequence number range.
    """
    if stream_list:
        valid_list = []
        for item in stream_list:
            stream_id = item.get("id")
            stream_time, stream_seq_num = stream_id.split("-")
            if int(stream_time) < int(from_stream_time):
                continue

            if int(stream_time) == int(from_stream_time):
                if not from_seq_num or int(stream_seq_num) >= int(from_seq_num):
                    valid_list.append(item)
            else:
                valid_list.append(item)

        response = f"*{len(valid_list)}\r\n"
        for valid_item in valid_list:
            len_dict = len(valid_item) - 1
            response += f"*{len_dict}\r\n"
            for key, value in valid_item.items():
                if key == "id":
                    response += redis_utils.convert_to_resp(valid_item.get("id"))
                else:
                    response += f"*{len_dict * 2}\r\n"
                    response += redis_utils.convert_to_resp(key)
                    response += redis_utils.convert_to_resp(value)

        client_socket.send(response.encode())


def xread_command_helper(message_arr: List[str], n_args: int, client_socket: socket, redis_streams_dict,
                         only_new_values=None):
    """
    Handles the XREAD command and retrieves a range of items from the stream, from the last received item to the specified end.

    Args:
        message_arr (List[str]): The list of command arguments.
        n_args (int): The number of arguments in the command.
        client_socket (socket): The client socket to send responses to.
        redis_streams_dict (dict): The dictionary of Redis streams.
        only_new_values (list): The list of new values to retrieve, if provided.
    """
    if message_arr[1].lower() == "streams":
        if only_new_values == []:
            client_socket.send("$-1\r\n".encode())
            return
        if only_new_values:
            stream_list_with_key = [(message_arr[2], only_new_values)]
            response = redis_utils.convert_xread_streams_to_resp(stream_list_with_key)
            client_socket.send(response.encode())
            return
        len_of_keys = int((len(message_arr) - 2) / 2)
        keys_list = message_arr[2:2 + len_of_keys]
        from_id_list = message_arr[2 + len_of_keys:]
        stream_list_with_key = []
        for idx, key in enumerate(keys_list):
            from_time, from_seq = redis_utils.find_time_and_seq(from_id_list[idx])
            list_values = redis_streams_dict.get(key)

            valid_values = []
            for item in list_values:
                stream_id = item["id"]
                stream_time, stream_seq_num = redis_utils.find_time_and_seq(stream_id)
                if int(stream_time) < int(from_time):
                    continue
                elif int(stream_time) == int(from_time):
                    if not from_seq or int(stream_seq_num) > int(from_seq):
                        valid_values.append(item)
                else:
                    valid_values.append(item)
            if not valid_values:
                client_socket.send("$-1\r\n".encode())
                return
            stream_list_with_key.append((key, valid_values))

        response = redis_utils.convert_xread_streams_to_resp(stream_list_with_key)
        client_socket.send(response.encode())


def handle_blocking_in_xread(message_arr: List[str]):
    """
    Handles the BLOCK argument in the XRDB command and waits until a new entry is added to a Redis stream.
    If the blocking time is 0, it waits until a new entry is added in a loop.
    If the blocking time is greater than 0, it waits for the specified amount of time before returning.

    Args:
        message_arr (List[str]): The list of command arguments.

    Returns:
        None
    """
    if message_arr[1].lower() == "block":
        block_time = float(message_arr[2]) / 1000
        if block_time == 0:
            redis_utils.wait_until_new_add_stream = True
            while redis_utils.wait_until_new_add_stream:
                time.sleep(0.1)
        else:
            time.sleep(block_time)
            redis_utils.can_add_redis_stream = False
        del message_arr[1:3]


def handle_dollar_in_xread(client_socket, n_args, message_arr: List[str], prev_copy_redis_streams_dict,
                           new_copy_redis_streams_dict):
    """
    Handles the $ argument in the XRDB command and checks if there are any new entries in the Redis stream.
    If there are no new entries, it sends a -1 response.
    If there are new entries, it sends the new entries in the XRDB stream format.

    Args:
        client_socket (socket): The client socket to send responses to.
        n_args (int): The number of arguments in the command.
        message_arr (List[str]): The list of command arguments.
        prev_copy_redis_streams_dict (dict): The previous copy of the Redis streams dictionary.
        new_copy_redis_streams_dict (dict): The new copy of the Redis streams dictionary.

    Returns:
        None
    """
    if message_arr[-1] == "$":
        if prev_copy_redis_streams_dict == new_copy_redis_streams_dict:
            client_socket.send("$-1\r\n".encode())
            return
        else:
            list1 = prev_copy_redis_streams_dict.get(message_arr[2])
            list2 = new_copy_redis_streams_dict.get(message_arr[2])

            diff2 = [item for item in list2 if item not in list1]
            xread_command_helper(message_arr, n_args, client_socket, new_copy_redis_streams_dict, diff2)
            return


def incr_command_helper(message_arr: List[str], n_args: int, client_socket: socket, addr: str,
                        is_multi_command: bool = False):
    """
    Handles the INCR command and increments the value associated with the given key in the Redis dictionary.
    If the value is not an integer, it sends an error response.

    Args:
        message_arr (List[str]): The list of command arguments.
        n_args (int): The number of arguments in the command.
        client_socket (socket): The client socket to send responses to.
        addr (str): The address of the client for IP sockets.
        is_multi_command (bool): Whether the command is part of a transaction block (MULTI/EXEC).

    Returns:
        None
    """
    key = message_arr[1]
    value = redis_utils.redis_dict.get(key, None)
    if value:
        try:
            value_int = int(value) + 1
            redis_utils.redis_dict.update({key: str(value_int)})
            if is_multi_command:
                redis_utils.queue_commands_response.get(addr).append(f":{value_int}\r\n")
                return
            client_socket.send(f":{value_int}\r\n".encode())
        except ValueError as e:
            if is_multi_command:
                redis_utils.queue_commands_response.get(addr).append("-ERR value is not an integer or out of range\r\n")
                return
            client_socket.send("-ERR value is not an integer or out of range\r\n".encode())
        except Exception as e:
            print(f"Exception found : {e}")
    else:
        redis_utils.redis_dict.update({key: "1"})
        if is_multi_command:
            redis_utils.queue_commands_response.get(addr).append(":1\r\n")
            return
        client_socket.send(":1\r\n".encode())
