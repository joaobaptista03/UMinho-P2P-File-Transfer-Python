import socket
import sys
import threading

class FSTracker:
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.node_files = {}

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
        exitFlag = False
        while exitFlag == False:
            chunk = client_socket.recv(1024).decode('utf-8')
            data += chunk
        
            if '<' in data:
                messages = data.split('<')
                for message in messages:
                    if message:
                        print(f"Received message from {client_address}: {message}")

                        if message.startswith("REGISTER"):
                            _, files = message.split(',')
                            self.register_node(files, client_address)

                            print(f"Node registered: {client_address[0]}:{client_address[1]}")

                        elif message.startswith("GET"):
                            filename = message[4:]
                            self.send_nodes_to_node(filename, client_address, client_socket)

                        elif message.startswith("EXIT"):
                            print("Node " + client_address[0] + " exited.")
                            if client_address[0] in self.node_files:
                                del self.node_files[client_address[0]]
                            client_socket.close()

                            exitFlag = True
                            
                        else:
                            print("Invalid Message.")

                data = ""
            
            if not chunk:
                break

    def register_node(self, files, client_address):
        if len(files) > 0:
            files_list = files.split(';')
            self.node_files[client_address[0]] = set(files_list)

    def send_nodes_to_node(self, filename, client_address, client_socket):
        nodes_with_file = [(node, files) for node, files in self.node_files.items() if filename in files and client_address[0] != node]
        
        if nodes_with_file:
            nodeResult = ""
            for (node, _) in nodes_with_file:
                nodeResult += node + ";"
            nodeResult = nodeResult[:-1]
            
            response = f"FILE_FOUND {filename}~{nodeResult}<"
            client_socket.send(response.encode('utf-8'))

            self.update_node_files(client_address, filename)

        else:
            response = f"FILE_NOT_FOUND {filename}<"
            client_socket.send(response.encode('utf-8'))

    def update_node_files(self, client_address, filename):
        if client_address[0] in self.node_files:
            self.node_files[client_address[0]].add(filename)
        else:
            self.node_files[client_address[0]] = {filename}

if __name__ == "__main__":
    args = sys.argv[1:]

    tracker = FSTracker(args[0], int(args[1]))
    tracker.start()
