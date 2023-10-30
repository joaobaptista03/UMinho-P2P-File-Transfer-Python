import socket
import threading

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
            print("TESTE1 -> " + str(client_address))
            client_thread = threading.Thread(target = self.handle_node_message, args = (client_socket, client_address))
            client_thread.start()

    def handle_node_message(self, client_socket, client_address):
        print("cheguei")    
        data = client_socket.recv(1024).decode('utf-8')
        print("li" + str(data))

        if data.startswith("REGISTER"):
            _, node_ip, node_port, files = data.split(',')
            files = files.split(';')  # Os nomes dos ficheiros são separados por ';'
            self.nodes[(node_ip, int(node_port))] = set(files)
            print(f"Node registered: {node_ip}:{node_port}")
        elif data.startswith("GET"):
            filename = data[4:]
            nodes_with_file = [(node, files) for node, files in self.nodes.items() if filename in files]
            print(nodes_with_file)
            print(client_address)
            if nodes_with_file:
                node_ip, node_port = nodes_with_file[0][0]
                response = f"FILE_FOUND {node_ip}:{node_port}"
                client_socket.send(response.encode('utf-8'))
            else:
                response = "FILE_NOT_FOUND"
                client_socket.send(response.encode('utf-8'))
        else:
            print("Invalid Message.")

if __name__ == "__main__":
    tracker_ip = "10.4.4.1"
    tracker_port = 9090

    tracker = FSTracker(tracker_ip, tracker_port)
    tracker.start()