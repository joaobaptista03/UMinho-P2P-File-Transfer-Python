import os
import socket
import sys
import threading
import time

class FSNode:
    def __init__(self, node_name, files_folder, tracker_ip, tracker_port):
        """
        Initializes a new instance of the FSNode class.
        """
        self.name = node_name
        self.ip = None
        self.port = None
        self.files_folder = files_folder

        self.tracker_name = None
        self.tracker_ip = tracker_ip
        self.tracker_port = tracker_port

        self.tcp_socket = None
        self.udp_socket = None

        self.nodes_responsetime = {}

    def start(self):
        """
        Starts the FSNode by connecting to the tracker, binding the UDP socket,
        and starting the necessary threads for handling node and tracker messages,
        as well as listening for requests.
        """
        self.connect_to_tracker()

        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_socket.bind((self.ip, 9090))

        threading.Thread(target=self.handle_node_message, daemon=True).start()
        threading.Thread(target=self.handle_tracker_message, daemon=True).start()
        th = threading.Thread(target=self.listen_for_requests, daemon=True)
        th.start()
        th.join()

    def connect_to_tracker(self):
        """
        Connects the FSNode to the tracker and registers the files in the specified folder.

        Args:
            files_folder (str): The path to the folder containing the files to be registered.
            tracker_ip (str): The IP address of the tracker.
            tracker_port (int): The port number of the tracker.

        Returns:
            None
        """

        self.tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_socket.bind((self.tcp_socket.getsockname()[0], 9090))
        self.tcp_socket.connect((self.tracker_ip, self.tracker_port))
        
        self.ip = self.tcp_socket.getsockname()[0]
        self.port = self.tcp_socket.getsockname()[1]

        files_str = self.get_files_list_string()
        self.send_tracker_message(f"REGISTER,{self.name},{files_str}")

    def get_files_list_string(self):
        """
        Returns a string containing the names of all files in the files folder, and creates the folder if it does not exist.

        Returns:
            str: A string containing the names of all files in the folder, separated by semicolons.
        """
        if not os.path.exists(self.files_folder):
            os.makedirs(self.files_folder)
        file_list = os.listdir(self.files_folder)
        files = ""

        for file in file_list:
            files += file + ";"
        files = files[:-1]
        
        return files
    
    def handle_tracker_message(self):
        """
        Handles the incoming messages from the tracker.
        Receives messages from the tracker and processes them accordingly.
        If the message is 'FILE_FOUND', it requests the download of the file from the fastest node.
        If the message is 'FILE_NOT_FOUND', it notifies that the file is not found in the network.
        Prints an error message for any other invalid message received.
        """
        data = ""
        while True:
            chunk = self.tcp_socket.recv(1024).decode('utf-8')
            data += chunk

            if '<' in data:
                messages = data.split('<')
                for message in messages:
                    if message:
                        if message.startswith("REGISTERED") and self.tracker_name is None:
                            _, tracker_name = message.split(',')
                            self.tracker_name = tracker_name
                            print(f"{self.name} at {self.ip}:{self.port} registered in {self.tracker_name} with the files: {self.get_files_list_string()}")

                        elif message.startswith("FILE_FOUND"):
                            filename = self.request_download(message)

                        elif message.startswith("FILE_NOT_FOUND"):
                            _, filename = message.split(" ", 1)
                            print(f"File '{filename}' not found in the network.")

                        else:
                            print("Invalid Message.")

                data = ""

            if not chunk:
                break

    def request_download(self, message):
        """
        Requests a download of a file from the fastest node.

        Args:
            message (str): The message containing the file name and available nodes.

        Returns:
            tuple: A tuple containing the filename and the fastest node.
        """
        _, info = message.split(" ", 1)
        file_and_nodes = info.split("~")
        filename = file_and_nodes[0]
        nodes_ip = file_and_nodes[1].split(";")

        fastest_node = nodes_ip[0]
        if len(nodes_ip) > 1:
            fastest_node = self.get_fastest_node(nodes_ip)

        self.send_node_message(f"DOWNLOAD_REQUEST,{filename},{self.name}", fastest_node)
        print("sent " + f"DOWNLOAD_REQUEST,{filename},{self.name}" + " to " + fastest_node)
        return filename
    
    def get_fastest_node(self, nodes):
        """
        Returns the fastest node from the given list of nodes.

        Parameters:
        - nodes (list): A list of nodes to compare.

        Returns:
        - fastest_node: The fastest node from the given list.
        """
        fastest_node = nodes[0]
        for node in nodes:
            self.send_node_message(f"PING;{str(time.time())}", node)

        while len(nodes) > len(self.nodes_responsetime):
            time.sleep(0.1)

        for node in nodes:
            if self.nodes_responsetime[node] < self.nodes_responsetime[fastest_node]:
                fastest_node = node
        
        self.nodes_responsetime.clear()
        
        return fastest_node

    def handle_node_message(self):
        """
        Handles the incoming messages from other nodes.

        Receives messages from other nodes and performs the appropriate actions based on the message type.
        The supported message types are:
        - DOWNLOAD_REQUEST: Sends the requested file to the sender node.
        - DOWNLOAD_RESPONSE: Writes the downloaded file from the sender node.
        - PING: Sends a ping response to the sender node.
        - PRESPONSE: Sets the response time for the ping request.

        Invalid messages are ignored.

        Returns:
        None
        """
        data = ""
        while True:
            chunk, sender_address = self.udp_socket.recvfrom(1024)
            data += chunk.decode('utf-8')

            if '<' in data:
                messages = data.split('<')
                for message in messages:
                    if message:
                        if message.startswith("DOWNLOAD_REQUEST"):
                            _, filename, node_name = message.split(',')
                            self.send_file(filename, sender_address)
                            print(f"File {filename} sent to {node_name}")

                        elif message.startswith("DOWNLOAD_RESPONSE"):
                            _, node_name, filename, response = message.split('~', 3)
                            self.write_file(filename, response, node_name)

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
        """
        Sends a file to the specified sender address.

        Args:
            filename (str): The name of the file to send.
            sender_address (tuple): The address of the sender in the format (ip, port).

        Returns:
            None
        """
        file_path = os.path.join("NodeFiles", filename)
        file_content = open(file_path, 'rb').read().decode("utf-8")

        sendMessage = "DOWNLOAD_RESPONSE~" + self.name + "~" + filename + "~" + file_content
        self.send_node_message(sendMessage, sender_address[0])

    def write_file(self, filename, response, node_name):
        """
        Writes the given response to a file with the specified filename.

        Args:
            filename (str): The name of the file to write.
            response (str): The content to write to the file.

        Returns:
            None
        """
        with open(f"NodeFiles/{filename}", 'wb') as file:
            file.write(response.encode("utf-8"))

        print(f"File {filename} downloaded from {node_name}")

    def send_presponse(self, start_time, sender_address):
        """
        Sends a PRESPONSE message to the specified sender address.

        Args:
            start_time (float): The start time of the request.
            sender_address (str): The address of the sender.

        Returns:
            None
        """
        self.send_node_message(f"PRESPONSE;{start_time}", sender_address[0])

    def set_response_time(self, start_time, sender_address):
        """
        Sets the response time for a specific sender address.

        Args:
            start_time (float): The start time of the request.
            sender_address (str): The address of the sender.

        Returns:
            None
        """
        time_diff = time.time() - start_time
        self.nodes_responsetime[sender_address[0]] = time_diff

    def listen_for_requests(self):
        """
        Listens for user requests and performs corresponding actions.

        The function prompts the user to enter a command, such as 'GET <filename>' or 'EXIT' to quit.
        It continuously listens for user input and performs the following actions:
        - If the user input starts with 'GET', it extracts the filename and sends a tracker message with the command.
        - If the user input is 'EXIT' (case-insensitive), it sends a tracker message with the command and closes the UDP socket.

        Returns:
            None
        """
        while True:
            user_input = input("Enter command (e.g., 'GET <filename>' or 'EXIT' to quit): \n")
            if user_input.startswith("GET"):
                filename = user_input[4:]
                self.send_tracker_message(f"GET,{filename}")

            elif user_input.upper() == "EXIT":
                self.send_tracker_message("EXIT")
                self.udp_socket.close()
                break

    def send_tracker_message(self, message):
        """
        Sends a message to the tracker.

        Args:
            message (str): The message to be sent.

        Returns:
            None
        """
        self.tcp_socket.send((message + "<").encode('utf-8'))

    def send_node_message(self, message, node):
        """
        Sends a message to a specified node address.

        Args:
            message (str): The message to be sent.
            node ((str, str)): The address and the name of the node to send the message to.

        Returns:
            None
        """
        self.udp_socket.sendto((message + "<").encode('utf-8'), (node, 9090))

if __name__ == "__main__":
    args = sys.argv[1:]

    files_folder = args[0]
    tracker_ip = args[1]
    tracker_port = int(args[2])
    node_name = args[3]

    node = FSNode(node_name, files_folder, tracker_ip, tracker_port)
    node.start()