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

    def connect_to_tracker(self, files_folder, tracker_ip, tracker_port):
        self.tracker_ip = tracker_ip
        self.tracker_port = tracker_port

        self.tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_socket.bind((self.tcp_socket.getsockname()[0], 9090))
        self.tcp_socket.connect((tracker_ip, tracker_port))
        
        self.ip = self.tcp_socket.getsockname()[0]
        self.port = self.tcp_socket.getsockname()[1]

        files_str = self.get_files_list_string(files_folder)
        self.send_tracker_message(f"REGISTER,{files_str}")

        print(f"Node at {self.ip}:{self.port} registered with the tracker with the files: {files_str}")

    def get_files_list_string(self, files_folder):
        if not os.path.exists(files_folder):
            os.makedirs(files_folder)
        file_list = os.listdir(files_folder)
        files = ""

        for file in file_list:
            files += file + ";"
        files = files[:-1]
        
        return files
    
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
                            (filename, fastest_node) = self.request_download(message)
                            print(f"File '{filename}' is available at {fastest_node}:9090")

                        elif message.startswith("FILE_NOT_FOUND"):
                            _, filename = message.split(" ", 1)
                            print(f"File '{filename}' not found in the network.")
                            
                        else:
                            print("Invalid Message.")

                data = ""
            
            if not chunk:
                break

    def request_download(self, message):
        _, info = message.split(" ", 1)
        file_and_nodes = info.split("~")
        filename = file_and_nodes[0]
        nodes = file_and_nodes[1].split(";")

        fastest_node = nodes[0]
        if len(nodes) > 1:
            fastest_node = self.get_fastest_node(nodes)

        self.send_node_message(f"DOWNLOAD_REQUEST,{filename}", fastest_node)
        return (filename, fastest_node)
    
    def get_fastest_node(self, nodes):
        fastest_node = nodes[0]
        for node in nodes:
            self.send_node_message(f"PING;{str(time.time())}", node)

        while len(nodes) > len(self.nodes_responsetime):
            time.sleep(0.1)

        for node in nodes:
            if self.nodes_responsetime[node] < self.nodes_responsetime[fastest_node]:
                fastest_node = node
        
        self.get_fastest_node.clear()
        
        return fastest_node

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
                            self.send_file(filename, sender_address)
                            print(f"File {filename} sent to {sender_address}")

                        elif message.startswith("DOWNLOAD_RESPONSE"):
                            _, filename, response = message.split('~', 2)
                            self.write_file(filename, response)

                            print(f"File '{filename}' downloaded from {sender_address}")

                        elif message.startswith("PING"):
                            _, start_time = message.split(';')
                            self.send_presponse(start_time, sender_address)
                            
                            print(f"Ping response sent to {sender_address}")
                        
                        elif message.startswith("PRESPONSE"):
                            _, start_time = message.split(';')
                            self.set_response_time(float(start_time), sender_address)

                        else:
                            print("Invalid Message.")

                data = ""
            
            if not chunk:
                break

    def send_file(self, filename, sender_address):
        file_path = os.path.join("NodeFiles", filename)
        file_content = open(file_path, 'rb').read().decode("utf-8")

        sendMessage = "DOWNLOAD_RESPONSE~" + filename + "~" + file_content
        self.send_node_message(sendMessage, sender_address[0])

    def write_file(self, filename, response):
        with open(f"NodeFiles/{filename}", 'wb') as file:
            file.write(response.encode("utf-8"))

    def send_presponse(self, start_time, sender_address):
        self.send_node_message(f"PRESPONSE;{start_time}", sender_address[0])

    def set_response_time(self, start_time, sender_address):
        time_diff = time.time() - start_time
        self.nodes_responsetime[sender_address] = time_diff

    def listen_for_requests(self):
        print("Enter command (e.g., 'GET <filename>' or 'EXIT' to quit): \n")
        while True:
            user_input = input()
            if user_input.startswith("GET"):
                filename = user_input[4:]
                self.send_tracker_message(f"GET,{filename}")

            elif user_input.upper() == "EXIT":
                self.send_tracker_message("EXIT")
                self.udp_socket.close()
                break

    def send_tracker_message(self, message):
        self.tcp_socket.send((message + "<").encode('utf-8'))
        print(f"Sent \"{message}\" to tracker")

    def send_node_message(self, message, node_address):
        self.udp_socket.sendto((message + "<").encode('utf-8'), (node_address, 9090))
        print(f"Sent \"{message}\" to {node_address}:9090")

if __name__ == "__main__":
    args = sys.argv[1:]

    node = FSNode()
    node.start()