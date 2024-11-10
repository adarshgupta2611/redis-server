import socket


def accept_client_concurrently(client_socket: socket, addr: str):
    """
    Accepts a client connection and handles it concurrently

    Args:
        client_socket (socket): Socket representing the connection
        addr (str): Address of the client for IP sockets
    """
    try:
        while client_socket.recv(1024) is not None:
            client_socket.send(b"+PONG\r\n")
    except Exception as e:
        print(f"Error occurred while handling client: {e}")
    finally:
        client_socket.close()
