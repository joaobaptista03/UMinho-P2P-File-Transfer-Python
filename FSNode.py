import os
import socket
import sys
import threading
import time

class FSNode:
    """
    Represents a file system node in a distributed file sharing network.

    Attributes:
        name (str): The name of the node.
        files_folder (str): The folder path where the files are stored.
        tracker_domain (str): The domain name or IP address of the tracker server.
        tracker_port (int): The port number of the tracker server.
        tcp_socket (socket.socket): The TCP socket used for communication with the tracker server.
        udp_socket (socket.socket): The UDP socket used for communication with other nodes.
        nodes_responsetime (dict): A dictionary to store the response times of other nodes.
        nodes_lookup (dict): A dictionary to store the IP addresses of other nodes.
    """
    
    def __init__(self, files_folder, tracker_domain, tracker_port):
        self.name = socket.gethostname() + ".cc2023"
        self.files_folder = files_folder

        self.tracker_domain = tracker_domain
        self.tracker_port = tracker_port

        self.tcp_socket = None
        self.udp_socket = None

        self.nodes_responsetime = {}
        self.nodes_lookup = {}

    def start(self):
        """
        Starts the FSNode by connecting to the tracker, binding the UDP socket,
        and starting the necessary threads for handling messages and listening for requests.
        """
        self.connect_to_tracker()

        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_socket.bind((self.name, self.tracker_port))

        threading.Thread(target=self.handle_node_message, daemon=True).start()
        threading.Thread(target=self.handle_tracker_message, daemon=True).start()
        th = threading.Thread(target=self.listen_for_requests, daemon=True)
        th.start()
        th.join()

    def connect_to_tracker(self):
            """
            Connects to the tracker server and registers the files of the FSNode.

            Returns:
                None
            """
            self.tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.tcp_socket.connect((self.tracker_domain, self.tracker_port))

            files_str = self.get_files_list_string()
            self.send_tracker_message(f"REGISTER,{files_str}")
            print(f"{self.name} registered in {self.tracker_domain} with the files: {files_str}")

    def get_files_list_string(self):
        """
        Returns a string representation of the list of files in the files_folder.

        Returns:
            str: A string representation of the list of files in the files_folder.
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
        Handles incoming messages from the tracker.

        Receives messages from the tracker and processes them accordingly.
        If a FILE_FOUND message is received, it requests the download of the specified file.
        If a FILE_NOT_FOUND message is received, it prints a message indicating that the file was not found.
        If an invalid message is received, it prints a message indicating that the message is invalid.
        """
        data = ""
        while True:
            chunk = self.tcp_socket.recv(1024).decode('utf-8')
            data += chunk

            if '<' in data:
                messages = data.split('<')
                for message in messages:
                    if message:
                        if message.startswith("FILE_FOUND"):
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
        Requests the download of a file from the fastest node.

        Args:
            message (str): The message containing the file information and node IPs.

        Returns:
            str: The filename of the requested file.
        """
        _, info = message.split(" ", 1)
        file_and_nodes = info.split("~")
        filename = file_and_nodes[0]
        nodes_ip = file_and_nodes[1].split(";")

        fastest_node = nodes_ip[0]
        if len(nodes_ip) > 1:
            fastest_node = self.get_fastest_node(nodes_ip)

        self.send_node_message(f"DOWNLOAD_REQUEST,{filename}", fastest_node)
        return filename

    def get_fastest_node(self, nodes):
        """
        Returns the fastest node from the given list of nodes.

        Parameters:
        - nodes (list): A list of nodes to compare.

        Returns:
        - fastest_node: The node with the fastest response time.
        """
        fastest_node = nodes[0]
        for node in nodes:
            self.send_node_message(f"PING;{str(time.time())}", node)

        while len(nodes) > len(self.nodes_responsetime):
            time.sleep(0.1)

        for node in nodes:
            print(f"{node}: {self.nodes_responsetime[node]}")
            if self.nodes_responsetime[node] < self.nodes_responsetime[fastest_node]:
                fastest_node = node

        self.nodes_responsetime.clear()

        return fastest_node

    def handle_node_message(self):
        """
        Handles incoming messages from other nodes.

        Receives messages over UDP socket and processes them based on their content.
        The messages can be of different types, such as DOWNLOAD_REQUEST, DOWNLOAD_RESPONSE,
        PING, PRESPONSE, or Invalid Message.

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
                        node_name = socket.gethostbyaddr(sender_address[0])[0]
                        
                        if message.startswith("DOWNLOAD_REQUEST"):
                            _, filename = message.split(',')
                            self.send_file(filename, node_name)
                            print(f"File {filename} sent to {node_name}")

                        elif message.startswith("DOWNLOAD_RESPONSE"):
                            _, filename, response = message.split('~')
                            self.write_file(filename, response, node_name)

                        elif message.startswith("PING"):
                            _, start_time = message.split(';')
                            self.send_presponse(start_time, node_name)

                            print(f"Ping response sent to {node_name}")

                        elif message.startswith("PRESPONSE"):
                            _, start_time = message.split(';')
                            self.set_response_time(float(start_time), node_name)

                        else:
                            print("Invalid Message.")

                data = ""

            if not chunk:
                break

    def send_file(self, filename, node_name):
        """
        Sends a file to a specified node.

        Args:
            filename (str): The name of the file to be sent.
            node_name (str): The name of the destination node.

        Returns:
            None
        """
        file_path = os.path.join("NodeFiles", filename)
        file_content = open(file_path, 'rb').read().decode("utf-8")

        sendMessage = "DOWNLOAD_RESPONSE~" + filename + "~" + file_content
        self.send_node_message(sendMessage, node_name)

    def write_file(self, filename, response, node_name):
        """
        Writes the given response to a file with the specified filename.

        Parameters:
        - filename (str): The name of the file to be written.
        - response (str): The content to be written to the file.
        - node_name (str): The name of the node from which the file is downloaded.

        Returns:
        None
        """
        with open(f"NodeFiles/{filename}", 'wb') as file:
            file.write(response.encode("utf-8"))

        print(f"File {filename} downloaded from {node_name}")

    def send_presponse(self, start_time, node_name):
        """
        Sends a PRESPONSE message to the specified node with the given start time.

        Args:
            start_time (float): The start time of the operation.
            node_name (str): The name of the destination node.

        Returns:
            None
        """
        self.send_node_message(f"PRESPONSE;{start_time}", node_name)

    def set_response_time(self, start_time, node_name):
        """
        Sets the response time for a given node.

        Args:
            start_time (float): The start time of the operation.
            node_name (str): The name of the node.

        Returns:
            None
        """
        time_diff = time.time() - start_time
        self.nodes_responsetime[node_name] = time_diff

    def listen_for_requests(self):
        """
        Listens for user input commands and performs corresponding actions.

        The function continuously prompts the user for input commands until the user enters 'EXIT' to quit.
        If the user enters a command starting with 'GET', the function extracts the filename from the command
        and sends a tracker message with the command 'GET' and the filename.
        If the user enters 'EXIT', the function sends a tracker message with the command 'EXIT' and closes the UDP socket.

        Args:
            None

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
        Sends a message to a specified node.

        Args:
            message (str): The message to be sent.
            node (str): The node to send the message to.

        Returns:
            None
        """
        if node not in self.nodes_lookup:
            self.nodes_lookup[node] = socket.gethostbyname(node)
        self.udp_socket.sendto((message + "<").encode('utf-8'), (self.nodes_lookup[node], 9090))

if __name__ == "__main__":
    args = sys.argv[1:]

    files_folder = args[0]
    tracker_domain = args[1]
    tracker_port = int(args[2])

    node = FSNode(files_folder, tracker_domain, tracker_port)
    node.start()
