import socket
import threading

# Classe que representa o rastreador (FS_Tracker)
class FSTracker:
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.nodes = []  # Lista de nós registados no rastreador

    def start(self):
        # Inicia o servidor do rastreador
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.ip, self.port))
        server_socket.listen(5)
        print(f"Tracker listening on {self.ip}:{self.port}")

        while True:
            client_socket, client_address = server_socket.accept()
            client_thread = threading.Thread(target=self.handle_node_registration, args=(client_socket,))
            client_thread.start()

    def handle_node_registration(self, client_socket):
        # Lida com o registo de um nó no rastreador
        data = client_socket.recv(1024)
        node_info = data.decode('utf-8').split(',')
        node_ip, node_port = node_info
        self.nodes.append((node_ip, int(node_port)))
        print(f"Node registered: {node_ip}:{node_port}")

if __name__ == "__main__":
    # Configurações do rastreador
    tracker_ip = "127.0.0.1"
    tracker_port = 9090

    tracker = FSTracker(tracker_ip, tracker_port)
    tracker.start()
