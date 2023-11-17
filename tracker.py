import socket
import sys
import threading
import time

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
                            if files:
                                files = files.split(';')
                            if files:
                                self.node_files[client_address[0]] = set(files)
                            
                            print(f"Node registered: {client_address[0]}:{client_address[1]}")

                        elif message.startswith("GET"):
                            filename = message[4:]
                            nodes_with_file = [(node, files) for node, files in self.node_files.items() if filename in files and client_address[0] != node]
                            if nodes_with_file:
                                nodeResult = ""
                                for (node, files) in nodes_with_file:
                                    nodeResult += node + ";"
                                nodeResult = nodeResult[:-1]
                                response = f"FILE_FOUND {nodeResult}"
                                client_socket.send(response.encode('utf-8'))

                                if client_address[0] in self.node_files:
                                    self.node_files[client_address[0]].add(filename)
                                else:
                                    self.node_files[client_address[0]] = {filename}

                            else:
                                response = "FILE_NOT_FOUND"
                                client_socket.send(response.encode('utf-8'))

                        elif message.startswith("EXIT"):
                            print("Node " + client_address[0] + " exited.")
                            del self.node_files[client_address[0]]
                            client_socket.close()

                            exitFlag = True
                            
                        else:
                            print("Invalid Message.")

                data = ""
            
            if not chunk:
                break

if __name__ == "__main__":
    args = sys.argv[1:]

    tracker_ip = args[0]
    tracker_port = int(args[1])

    tracker = FSTracker(tracker_ip, tracker_port)
    tracker.start()
