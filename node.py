import socket

# Classe que representa um nó da rede (FS_Node)
class FSNode:
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.files = {}  # Dicionário de ficheiros que este nó possui

    def connect_to_tracker(self, tracker_ip, tracker_port):
        # Simula a conexão com o rastreador (tracker)
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((tracker_ip, tracker_port))
        registration_data = f"{self.ip},{self.port}"
        client_socket.send(registration_data.encode('utf-8'))
        client_socket.close()
        print(f"Node at {self.ip}:{self.port} registered with the tracker")

    def download_file(self, filename):
        # Simula o processo de download de um ficheiro
        print(f"Node at {self.ip}:{self.port} is downloading {filename}")

if __name__ == "__main__":
    # Configurações do nó
    node_ip = "127.0.0.2"
    node_port = 5001

    node = FSNode(node_ip, node_port)

    # Configurações do rastreador
    tracker_ip = "127.0.0.1"
    tracker_port = 9090

    node.connect_to_tracker(tracker_ip, tracker_port)

    # Mantém o nó em execução indefinidamente
    while True:
        pass
