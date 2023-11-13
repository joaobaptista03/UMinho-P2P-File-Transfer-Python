import socket
import threading

class FSTracker:
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.nodes = {}  # Dicionário de nós (peers) e ficheiros que eles possuem
        self.lock = threading.Lock()

    def start(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.ip, self.port))
        server_socket.listen(5)
        print(f"Tracker listening on {self.ip}:{self.port}")

        while True:
            client_socket, client_address = server_socket.accept()
            client_thread = threading.Thread(target = self.handle_node_message, args = (client_socket, client_address))
            client_thread.start()

    def handle_node_message(self, client_socket, client_address):
        data = ""
        while True:
            chunk = client_socket.recv(1024).decode('utf-8')
            data += chunk
        
            if '<' in data:
                messages = data.split('<')
                for message in messages:
                    if message:
                        print(f"Received message from {client_address}: {message}")

                        if message.startswith("REGISTER"):
                            _, node_ip, node_port, files = message.split(',')
                            files = files.split(';')
                            self.nodes[(node_ip, int(node_port))] = set(files)
                            print(f"Node registered: {node_ip}:{node_port}")
                        elif message.startswith("GET"):
                            filename = message[4:]
                            nodes_with_file = [(node, files) for node, files in self.nodes.items() if filename in files and client_address != node]
                            if nodes_with_file:
                                node_ip, node_port = nodes_with_file[0][0]
                                response = f"FILE_FOUND {node_ip}:{node_port}"
                                client_socket.send(response.encode('utf-8'))
                            else:
                                response = "FILE_NOT_FOUND"
                                client_socket.send(response.encode('utf-8'))
                        else:
                            print("Invalid Message.")

                data = ""
            
            if not chunk:
                break

if __name__ == "__main__":
    tracker_ip = "10.4.4.1"
    tracker_port = 9090

    tracker = FSTracker(tracker_ip, tracker_port)
    tracker.start()