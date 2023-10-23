import socket
import threading

# Classe que representa o rastreador (FS_Tracker)
class FSTracker:
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.nodes = {}  # Dicionário de nós (peers) e ficheiros que eles possuem

    def start(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.ip, self.port))
        server_socket.listen(5)
        print(f"Tracker listening on {self.ip}:{self.port}")

        while True:
            client_socket, client_address = server_socket.accept()
            client_thread = threading.Thread(target=self.handle_node_message, args=(client_socket,))
            client_thread.start()

    def handle_node_message(self, client_socket):
        data = client_socket.recv(1024).decode('utf-8')
        print("TESTE 1 -> " + str(data))
        if data.startswith("REGISTER"):
            _, node_ip, node_port, files = data.split(',')
            print("TESTE 2 -> " + str(node_ip))
            print("TESTE 3 -> " + str(node_port))
            print("TESTE 4 -> " + str(files))
            files = files.split(';')  # Os nomes dos ficheiros são separados por ';'
            self.nodes[(node_ip, int(node_port))] = set(files)
            print(f"Node registered: {node_ip}:{node_port}")
        else:
            # Aqui, trata consultas de ficheiros
            # Por exemplo, 'GET File1'
            # Implemente a lógica para responder a consultas de ficheiros aqui
            filename = data[4:]  # Remove "GET " para obter o nome do ficheiro
            nodes_with_file = [(node, files) for node, files in self.nodes.items() if filename in files]
            if nodes_with_file:
                node_ip, node_port = nodes_with_file[0][0]
                response = f"FILE_FOUND {node_ip}:{node_port}"
                client_socket.send(response.encode('utf-8'))
            else:
                response = "FILE_NOT_FOUND"
                client_socket.send(response.encode('utf-8'))

    def query_file(self, filename):
        # Simula a consulta do nó ao rastreador para obter informações sobre o ficheiro
        nodes_with_file = [node for node, files in self.nodes.items() if filename in files]
        if nodes_with_file:
            return nodes_with_file[0]
        return None

if __name__ == "__main__":
    # Configurações do rastreador
    tracker_ip = "127.0.0.1"
    tracker_port = 9090

    tracker = FSTracker(tracker_ip, tracker_port)
    tracker.start()