import os
import socket
import threading

class FSNode:
    def __init__(self):
        self.ip = None
        self.port = None
        self.tracker_ip = None
        self.tracker_port = None

        self.tcp_socket = None
        self.udp_socket = None

    def start(self):
        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_socket.bind((self.ip, 9090))

        udp_listener_thread = threading.Thread(target=self.listen_for_udp_messages, daemon=True)
        udp_listener_thread.start()

    def listen_for_udp_messages(self):
        while True:
            data, sender_address = self.udp_socket.recvfrom(1024)
            if not data:
                break
            message = data.decode('utf-8')
            
            if message.startswith("DOWNLOAD_REQUEST"):
                _, filename = message.split(',')
                filename = filename[:-1]
                sendMessage = "DOWNLOAD_RESPONSE," + filename + "," + self.read_file_content(filename)
                self.udp_socket.sendto(sendMessage.encode('utf-8'), (sender_address[0], 9090))
                print(f"{filename} sent to {sender_address}")

            if message.startswith("DOWNLOAD_RESPONSE"):
                _, filename, response = message.split(',')
                with open(f"NodeFiles/{filename}", 'wb') as file:
                    file.write(response.encode("utf-8"))

    def listen_for_requests(self):
        while True:
            user_input = input("Enter command (e.g., 'GET <filename>' or 'EXIT' to quit): \n")
            if user_input.upper() == "EXIT":
                self.exit()
                break
            elif user_input.startswith("GET "):
                filename = user_input[4:]
                self.download_file(filename)

    def connect_to_tracker(self, tracker_ip, tracker_port):
        self.tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_socket.connect((tracker_ip, tracker_port))

        self.tracker_ip = tracker_ip
        self.tracker_port = tracker_port
        self.ip = self.tcp_socket.getsockname()[0]
        self.port = self.tcp_socket.getsockname()[1]

        file_list = os.listdir("NodeFiles")
        files = ""
        for file in file_list:
            files += file + ";"
        files = files[:-1]
        registration_data = f"REGISTER,{self.ip},{self.port},{files}<"
        self.tcp_socket.send(registration_data.encode('utf-8'))

        if self.tcp_socket.recv(1024).decode() == ("PING"):
            self.tcp_socket.send("PING_RESPONSE".encode("utf-8"))

        print(f"Node at {self.ip}:{self.port} registered with the tracker")

    def query_tracker(self, query_data):
        self.tcp_socket.send(query_data.encode('utf-8'))
        response = self.tcp_socket.recv(1024).decode('utf-8')
        return response

    def download_file(self, filename):
        if not self.tracker_ip or not self.tracker_port:
            print("Erro: informações do rastreador em falta. Conecte-se ao rastreador primeiro.")
            return

        query_data = f"GET,{filename}<"
        tracker_response = self.query_tracker(query_data)

        if tracker_response.startswith("FILE_FOUND"):
            _, node_ip_port = tracker_response.split(" ", 1)
            node_ip, node_port = node_ip_port.split(":")
            print(f"File '{filename}' is available at {node_ip}:{node_port}")   
            self.download_from_node(node_ip, 9090, filename)

        else:
            print(f"File '{filename}' not found in the network.")
    
    def download_from_node(self, node_ip, node_port, filename):
        request_message = f"DOWNLOAD_REQUEST,{filename}<"
        self.udp_socket.sendto(request_message.encode('utf-8'), (node_ip, node_port))

        print(f"File '{filename}' downloaded from {node_ip}:{node_port}")

    def read_file_content(self, filename):
        file_path = os.path.join("NodeFiles", filename)
        with open(file_path, 'rb') as file:
            file_content = file.read()
        return file_content.decode('utf-8')
    
    def exit(self):
        self.tcp_socket.send("EXIT<".encode("utf-8"))

        self.udp_socket.close()

if __name__ == "__main__":
    node = FSNode()

    tracker_ip = "10.4.4.1"
    tracker_port = 9090

    node.connect_to_tracker(tracker_ip, tracker_port)

    node.start()

    request_listener_thread = threading.Thread(target=node.listen_for_requests, daemon=True)
    request_listener_thread.start()
    request_listener_thread.join()