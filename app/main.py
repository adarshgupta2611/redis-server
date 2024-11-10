import socket  # noqa: F401


def main():
    with socket.create_server(("localhost", 6379), reuse_port=True) as server_socket:
        client_socket, addr = server_socket.accept()
        client_socket.send(b"+PONG\r\n")


if __name__ == "__main__":
    main()
