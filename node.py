import os
import socket
import sys
import threading
import time

class FSNode:
    def __init__(self):
        self.ip = None
        self.port = None
        self.tracker_ip = None
        self.tracker_port = None
        self.tcp_socket = None
        self.udp_socket = None
        self.nodes_responsetime = {}

    def start(self):
        self.connect_to_tracker(args[0], args[1], int(args[2]))

        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_socket.bind((self.ip, 9090))

        threading.Thread(target=self.handle_node_message, daemon=True).start()
        threading.Thread(target=self.handle_tracker_message, daemon=True).start()
        
        th = threading.Thread(target=self.listen_for_requests, daemon=True)
        th.start()
        th.join()
    
    def handle_tracker_message(self):
        data = ""
        while True:
            chunk = self.tcp_socket.recv(1024).decode('utf-8')
            data += chunk
        
            if '<' in data:
                messages = data.split('<')
                for message in messages:
                    if message:
                        print(f"Received message from tracker: {message}")

                        if message.startswith("FILE_FOUND"):
                            _, info = message.split(" ", 1)
                            file_and_nodes = info.split("~")
                            filename = file_and_nodes[0]
                            nodes = file_and_nodes[1].split(";")

                            fastest_node = nodes[0]
                            for node in nodes:
                                if node not in self.nodes_responsetime:
                                    self.udp_socket.sendto(f"PING;{str(time.time())}<".encode('utf-8'), (node, 9090))
                                while node not in self.nodes_responsetime:
                                    time.sleep(0.1)
                                if self.nodes_responsetime[node] < self.nodes_responsetime[fastest_node]:
                                    fastest_node = node

                            print(f"File '{filename}' is available at {fastest_node}:9090")
                            
                            self.udp_socket.sendto(f"DOWNLOAD_REQUEST,{filename}<".encode('utf-8'), (nodes[0], 9090))

                        elif message.startswith("FILE_NOT_FOUND"):
                            _, filename = message.split(" ", 1)
                            print(f"File '{filename}' not found in the network.")
                            
                        else:
                            print("Invalid Message.")

                data = ""
            
            if not chunk:
                break


    def handle_node_message(self):
        data = ""
        while True:
            chunk, sender_address = self.udp_socket.recvfrom(1024)
            data += chunk.decode('utf-8')
        
            if '<' in data:
                messages = data.split('<')
                for message in messages:
                    if message:
                        print(f"Received message from node {sender_address[0]}: {message}")

                        if message.startswith("DOWNLOAD_REQUEST"):
                            _, filename = message.split(',')

                            file_path = os.path.join("NodeFiles", filename)
                            file_content = open(file_path, 'rb').read().decode("utf-8")

                            sendMessage = "DOWNLOAD_RESPONSE~" + filename + "~" + file_content + "<"
                            self.udp_socket.sendto(sendMessage.encode('utf-8'), (sender_address[0], 9090))
                            print(f"{filename} sent to {sender_address}")

                        elif message.startswith("DOWNLOAD_RESPONSE"):
                            _, filename, response = message.split('~', 2)

                            with open(f"NodeFiles/{filename}", 'wb') as file:
                                file.write(response.encode("utf-8"))

                            print(f"File '{filename}' downloaded from {sender_address[0]}:9090")

                        elif message.startswith("PING"):
                            _, start_time = message.split(';')
                            self.udp_socket.sendto(f"PRESPONSE;{start_time}<".encode('utf-8'), (sender_address[0], 9090))

                        elif message.startswith("PRESPONSE"):
                            _, start_time = message.split(';')
                            start_time_float = float(start_time)
                            time_diff = time.time() - start_time_float

                            self.nodes_responsetime[sender_address[0]] = time_diff

                        else:
                            print("Invalid Message.")

                data = ""
            
            if not chunk:
                break

    def listen_for_requests(self):
        while True:
            user_input = input("Enter command (e.g., 'GET <filename>' or 'EXIT' to quit): \n")
            if user_input.startswith("GET"):
                filename = user_input[4:]
                self.tcp_socket.send(f"GET,{filename}<".encode('utf-8'))

            elif user_input.upper() == "EXIT":
                self.tcp_socket.send("EXIT<".encode("utf-8"))
                self.udp_socket.close()
                break

    def connect_to_tracker(self, filesFolder, tracker_ip, tracker_port):
        self.tracker_ip = tracker_ip
        self.tracker_port = tracker_port

        self.tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_socket.bind((self.tcp_socket.getsockname()[0], 9090))
        self.tcp_socket.connect((tracker_ip, tracker_port))
        
        self.ip = self.tcp_socket.getsockname()[0]
        self.port = self.tcp_socket.getsockname()[1]

        file_list = os.listdir(filesFolder)
        files = ""
        for file in file_list:
            files += file + ";"
        files = files[:-1]
        registration_data = f"REGISTER,{files}<"
        self.tcp_socket.send(registration_data.encode('utf-8'))

        print(f"Node at {self.ip}:{self.port} registered with the tracker")

if __name__ == "__main__":
    args = sys.argv[1:]

    node = FSNode()
    node.start()
