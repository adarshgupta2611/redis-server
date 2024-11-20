import copy
from dataclasses import dataclass
import socket
import time
from typing import List, NamedTuple, Tuple
from app import redis_commands
from app import redis_utils


class Token(NamedTuple):
    type: str
    data: bytes


@dataclass
class ConnContext:
    id: int
    conn: socket.socket


def perform_handshake_with_master(m_conn, port: int):
    buf = b""

    with m_conn:
        m_conn.send("*1\r\n$4\r\nping\r\n".encode())
        token, buf = get_token(m_conn, buf)
        if token != Token("+", b"PONG"):
            print("Sync err: didn't get PONG")
            return
        m_conn.send(
            redis_utils.convert_to_resp(f"replconf listening-port {port}").encode()
        )
        token, buf = get_token(m_conn, buf)
        if token != Token("+", b"OK"):
            print("Sync err: didn't get OK for listening port")
            return
        m_conn.send(redis_utils.convert_to_resp("replconf capa psync2").encode())
        token, buf = get_token(m_conn, buf)
        if token != Token("+", b"OK"):
            print("Sync err: didn't get OK for capa")
            return
        m_conn.send(redis_utils.convert_to_resp("psync ? -1").encode())
        token, buf = get_token(m_conn, buf)
        resp_arr = token.data.split(b" ")
        if resp_arr[0] != b"FULLRESYNC":
            print("Sync err: didn't get FULLRESYNC for psync")
            return

        token, buf = get_token(m_conn, buf)

        if token.type != "$":
            print("Sync err: didn't get RDB for psync")
            return
        client_loop(m_conn, True, buf)


def client_loop(conn: socket.socket, from_master: bool = False, prev_buf: bytes = b""):
    print(f"Client loop start {conn}")
    cctx = ConnContext(conn.fileno(), conn)
    with conn:
        buf = prev_buf
        while True:
            try:
                cmd: list[bytes] = []
                token, buf = get_token(conn, buf)
                if token.type != "*":
                    continue
                arr_len = int(token.data)
                if arr_len < 1:
                    continue
                for _ in range(arr_len):
                    token, buf = get_token(conn, buf)
                    cmd.append(token.data)
                print(f"Got command: {cmd}")
                cmd_str = [byte.decode('utf-8') for byte in cmd]
                if cmd_str[0].lower() == "set":
                    redis_commands.set_command_helper(cmd_str, len(cmd), conn , True)
                elif cmd_str[0].lower() == "replconf" and cmd_str[1].lower() == "getack":
                    msg = f"REPLCONF ACK {redis_utils.replica_ack_offset}"
                    resp_msg = redis_utils.convert_to_resp(msg)
                    resp_msg_bytes = resp_msg.encode()
                    conn.send(resp_msg_bytes)
                    
                redis_utils.replica_ack_offset = redis_utils.replica_ack_offset + len(redis_utils.convert_to_resp(" ".join(cmd_str),True).encode())
            except (ConnectionError, AssertionError):
                break
    print(f"Client loop stop {conn}")


def get_token(
    conn: socket.socket,
    buf: bytes,
    fixed_size: int | None = None,
    fixed_type: str | None = None,
) -> tuple[Token, bytes]:
    while True:
        if fixed_size:
            assert fixed_type
            if len(buf) >= fixed_size:
                skip_len = 0
                if buf[fixed_size : fixed_size + 2] == b"\r\n":
                    skip_len = 2
                return (
                    Token(fixed_type, buf[:fixed_size]),
                    buf[fixed_size + skip_len :],
                )
        else:
            cmd_end = buf.find(b"\r\n")
            if cmd_end != -1:
                res = (Token(chr(buf[0]), buf[1:cmd_end]), buf[cmd_end + 2 :])
                if chr(buf[0]) in "$!=":
                    res = get_token(conn, res[1], int(res[0].data), res[0].type)
                return res
        recv_buf = conn.recv(1024)
        buf += recv_buf
        print(f"Recv {buf}")
        if not recv_buf:
            raise ConnectionError


def accept_client_concurrently(client_socket: socket, addr: str):
    """
    Accepts a client connection and handles it concurrently

    Args:
        client_socket (socket): Socket representing the connection
        addr (str): Address of the client for IP sockets
    """
    try:
        print("Inside accept_client_concurrently")
        while True:
            data: bytes = client_socket.recv(1024)
            if not data:
                break
            message: str = data.decode("utf-8")
            msg_arr, number_of_args = parse_message(message)
            choose_argument_and_send_output(msg_arr, number_of_args, client_socket, addr)
    except Exception as e:
        print(f"Error occurred while handling client: {e}")
    finally:
        client_socket.close()


def parse_message(message: str) -> Tuple[List[str], int]:
    """
    Parses a Redis protocol message and extracts the arguments and number of arguments

    Example:
        parse_message("*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$5\r\nmyvalue\r\n") -> (['SET', 'mykey', 'myvalue'], 3)
        parse_message("*1\r\n$4\r\nPING\r\n") -> (['PING'], 1)

    Args:
        message (str): The Redis protocol message

    Returns:
        Tuple[List[str], int]: The arguments and the number of arguments
    """
    msg_arr: List[str] = message.split("\r\n")
    number_of_args: int = int(msg_arr.pop(0).removeprefix("*"))
    msg_arr.remove(msg_arr[0])
    args_arr = msg_arr[::2]
    return (args_arr, number_of_args)


def choose_argument_and_send_output(
    message_arr: List[str], n_args: int, client_socket: socket, addr: str
):
    """
    Chooses the appropriate argument and sends the output to the client based on the command

    Example:
        choose_argument_and_send_output(['SET', 'mykey', 'myvalue'], 3, client_socket)
        choose_argument_and_send_output(['PING'], 1, client_socket)

    Args:
        message_arr (List[str]): The arguments extracted from the Redis protocol message
        n_args (int): The number of arguments
        client_socket (socket): The socket representing the client connection
    """
    if message_arr[0].lower() == "ping":
        client_socket.send(b"+PONG\r\n")
    elif message_arr[0].lower() == "echo":
        resp_msg = redis_utils.convert_to_resp(message_arr[1])
        client_socket.send(resp_msg.encode())
    elif message_arr[0].lower() == "set":
        redis_commands.set_command_helper(message_arr, n_args, client_socket)
    elif message_arr[0].lower() == "get":
        if redis_utils.dir or redis_utils.dbfilename:
            redis_commands.rdb_get_command_helper(message_arr, n_args, client_socket)
        else:
            redis_commands.get_command_helper(message_arr, n_args, client_socket)
    elif message_arr[0].lower() == "config":
        redis_commands.config_get_command_helper(message_arr, n_args, client_socket)
    elif message_arr[0].lower() == "keys":
        redis_commands.keys_get_command_helper(message_arr, n_args, client_socket)
    elif message_arr[0].lower() == "info":
        redis_commands.info_command_helper(message_arr, n_args, client_socket)
    elif message_arr[0].lower() == "replconf":
        if message_arr[1].lower() == "listening-port" or message_arr[1].lower()=="capa":
            client_socket.send("+OK\r\n".encode())
        elif message_arr[1].lower()=="ack":
            redis_utils.num_replicas_ack += 1
    elif message_arr[0].lower() == "psync":
        if message_arr[1] == "?" and message_arr[2] == "-1":

            client_socket.send(
                "+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n".encode()
            )
            rdb_hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
            rdb_content = bytes.fromhex(rdb_hex)
            rdb_length = f"${len(rdb_content)}\r\n".encode()
            client_socket.send(rdb_length + rdb_content)
            redis_utils.replica_sockets.update({addr:client_socket})
    elif message_arr[0].lower() == "wait":
        redis_commands.wait_command_helper(message_arr, n_args, client_socket)
    elif message_arr[0].lower() == "type":
        redis_commands.type_command_helper(message_arr, n_args, client_socket)
    elif message_arr[0].lower() == "xadd":
        redis_commands.xadd_command_helper(message_arr, n_args, client_socket)
    elif message_arr[0].lower() == "xrange":
        redis_commands.xrange_command_helper(message_arr, n_args, client_socket)    
    elif message_arr[0].lower() == "xread":
        prev_copy_redis_streams_dict = copy.deepcopy(redis_utils.redis_streams_dict)
        redis_commands.handle_blocking_in_xread(message_arr)
        new_copy_redis_streams_dict = copy.deepcopy(redis_utils.redis_streams_dict)
        redis_commands.handle_dollar_in_xread(client_socket,n_args,message_arr, prev_copy_redis_streams_dict, new_copy_redis_streams_dict)
        redis_commands.xread_command_helper(message_arr, n_args, client_socket,new_copy_redis_streams_dict)
    elif message_arr[0].lower() == "incr":
        redis_commands.incr_command_helper(message_arr, n_args, client_socket)
    elif message_arr[0].lower() == "multi":
        redis_commands.multi_command_helper(message_arr, n_args, client_socket)
    elif message_arr[0].lower() == "exec":
        client_socket.send("-ERR EXEC without MULTI\r\n".encode())
        
    
