import socket
import threading

from app import redis_utils
from .redis_utils import redis_args_parse
from .routes import accept_client_concurrently, perform_handshake_with_master


def main():
    """
    Main function for Redis Creating Server at the port 6379
    Create a server socket and bind to the port
    The 'reuse_port=True' option allows multiple connections to the same port
    This is useful when multiple clients connect simultaneously
    """
    redis_args_parse()
    if redis_utils.replicaof:
        replica = redis_utils.replicaof.split(" ")
        master_socket = socket.create_connection((replica[0], int(replica[1])))
        threading.Thread(target=perform_handshake_with_master, args=(master_socket, redis_utils.port),
                         daemon=True).start()

    with socket.create_server(("localhost", redis_utils.port), reuse_port=True) as server_socket:
        server_socket.listen()

        while True:
            client_socket, address = server_socket.accept()
            client_thread = threading.Thread(
                target=accept_client_concurrently, args=(client_socket, address)
            )
            client_thread.start()


if __name__ == "__main__":
    main()
