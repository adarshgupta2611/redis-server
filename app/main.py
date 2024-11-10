import socket
import threading
from .utils import accept_client_concurrently

def main():
    """
    Main function for Redis Creating Server at the port 6379
    Create a server socket and bind to the port
    The 'reuse_port=True' option allows multiple connections to the same port
    This is useful when multiple clients connect simultaneously
    """
    
    with socket.create_server(("localhost", 6379), reuse_port=True) as server_socket:
        server_socket.listen()
        
        while True:
            client_socket, address = server_socket.accept()
            client_thread = threading.Thread(target=accept_client_concurrently,args=(client_socket,address))
            client_thread.start()


if __name__ == "__main__":
    main()
