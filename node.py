import socket

# Classe que representa um nó da rede (FS_Node)
class FSNode:
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.files = {}  # Dicionário de ficheiros que este nó possui
        self.tracker_ip = None
        self.tracker_port = None

    def set_tracker_info(self, tracker_ip, tracker_port):
        self.tracker_ip = tracker_ip
        self.tracker_port = tracker_port

    def connect_to_tracker(self, tracker_ip, tracker_port):
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((tracker_ip, tracker_port))
        file2 = "File1"
        registration_data = f"REGISTER,{self.ip},{self.port},{file2}"
        client_socket.send(registration_data.encode('utf-8'))
        client_socket.close()
        node.set_tracker_info(tracker_ip, tracker_port)

        print(f"Node at {self.ip}:{self.port} registered with the tracker")

    def download_file(self, filename):
        if not self.tracker_ip or not self.tracker_port:
            print("Erro: informações do rastreador em falta. Conecte-se ao rastreador primeiro.")
            return

        query_data = f"GET {filename}"
        tracker_response = self.query_tracker(query_data)

        if tracker_response.startswith("FILE_FOUND"):
            _, node_ip_port = tracker_response.split(" ", 1)
            node_ip, node_port = node_ip_port.split(":")
            print(f"File '{filename}' is available at {node_ip}:{node_port}")
            self.download_from_node(node_ip, int(node_port), filename)
        else:
            print(f"File '{filename}' not found in the network.")

    def query_tracker(self, query_data):
        # Simula a consulta ao rastreador usando uma conexão TCP
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((self.tracker_ip, self.tracker_port))
        client_socket.send(query_data.encode('utf-8'))
        response = client_socket.recv(1024).decode('utf-8')
        client_socket.close()
        return response

    def download_from_node(self, node_ip, node_port, filename):
        # Implementar download do ficheiro do nó específico por UDP
        # Abrir uma nova conexão UDP e iniciar o download
        print(f"Node at {self.ip}:{self.port} is downloading '{filename}' from {node_ip}:{node_port}")

if __name__ == "__main__":
    node_ip = "127.0.0.2"
    node_port = 9091

    node = FSNode(node_ip, node_port)

    # Configurações do rastreador
    tracker_ip = "127.0.0.1"
    tracker_port = 9090

    node.connect_to_tracker(tracker_ip, tracker_port)

    # Exemplo de download de um ficheiro
    node.download_file("File1")

    while True:
        pass