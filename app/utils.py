import socket
from typing import List, Tuple


def accept_client_concurrently(client_socket: socket, addr: str):
    """
    Accepts a client connection and handles it concurrently

    Args:
        client_socket (socket): Socket representing the connection
        addr (str): Address of the client for IP sockets
    """
    try:
        while True:
            data: bytes = client_socket.recv(1024)
            if not data:
                break
            message: str = data.decode("utf-8")
            msg_arr, number_of_args = parse_message(message)
            choose_argument_and_send_output(msg_arr, number_of_args, client_socket)
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
    message_arr: List[str], n_args: int, client_socket: socket
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
        resp_msg = convert_to_resp(message_arr[1])
        client_socket.send(resp_msg.encode())


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
