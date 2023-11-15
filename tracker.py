import socket
import threading
import time

class FSTracker:
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.node_files = {}
        self.node_responsetime = {}

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
                            _, node_ip, node_port, files = message.split(',')
                            files = files.split(';')
                            self.node_files[(node_ip, int(node_port))] = set(files)
                            
                            responseTime = self.calculate_response_time(client_socket)
                            self.node_responsetime[(node_ip, int(node_port))] = responseTime
                            
                            print(f"Node registered: {node_ip}:{node_port}")

                        elif message.startswith("GET"):
                            filename = message[4:]
                            nodes_with_file = [(node, files) for node, files in self.node_files.items() if filename in files and client_address != node]
                            if nodes_with_file:
                                fastestNode = nodes_with_file[0][0]
                                for (node, files) in nodes_with_file:
                                    if self.node_responsetime[node] < self.node_responsetime[fastestNode]:
                                        fastestNode = node
                                node_ip, node_port = fastestNode
                                response = f"FILE_FOUND {node_ip}:{node_port}"
                                client_socket.send(response.encode('utf-8'))

                            else:
                                response = "FILE_NOT_FOUND"
                                client_socket.send(response.encode('utf-8'))

                        elif message.startswith("EXIT"):
                            print("Node " + client_address[0] + " exited.")
                            del self.node_files[client_address]
                            del self.node_responsetime[client_address]
                            client_socket.close()

                            exitFlag = True
                            
                        else:
                            print("Invalid Message.")

                data = ""
            
            if not chunk:
                break

    def calculate_response_time(self, client_socket):
        start_time = time.time()

        client_socket.send("PING".encode('utf-8'))
        if client_socket.recv(1024).decode() == ("PING_RESPONSE"):
            return time.time() - start_time

        return None

if __name__ == "__main__":
    tracker_ip = "10.4.4.1"
    tracker_port = 9090

    tracker = FSTracker(tracker_ip, tracker_port)
    tracker.start()