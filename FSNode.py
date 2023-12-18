import os
import socket
import sys
import threading
import time
import hashlib

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
        node_blocks (dict): A dictionary to store the blocks of a node.
        blocks (dict): A dictionary to store the blocks of a file.
        current_sending_blocks (dict): A dictionary to store the blocks that are currently being sent.
        exit (bool): A boolean value indicating whether the node should exit or not.

        TODO's:
        - If response time ping exceed a certain amount of time, remove node from network after 2nd try;
        - 
    """
    
    def __init__(self, files_folder, tracker_domain, tracker_port):
        self.MAX_CHAR_BLOCK = 32
        self.name = socket.gethostname() + ".cc2023"
        self.files_folder = files_folder

        self.tracker_domain = tracker_domain
        self.tracker_port = tracker_port

        self.tcp_socket = None
        self.udp_socket = None

        self.nodes_responsetime = {}
        self.nodes_lookup = {}

        self.node_blocks = {}
        self.blocks = {}

        self.current_sending_blocks = {}

        self.exit = False

    def start(self):
        """
        Starts the FSNode by connecting to the tracker, binding the UDP socket,
        and starting the necessary threads for handling messages and listening for requests.
        """
        self.connect_to_tracker()

        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_socket.bind((self.name, self.tracker_port))

        threading.Thread(target=self.handle_node_chunks, daemon=True).start()
        threading.Thread(target=self.handle_tracker_chunks, daemon=True).start()
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

    def handle_tracker_chunks(self):
        """
        Handles incoming messages from the tracker.

        Receives messages from the tracker and processes them, in another thread, accordingly.
        If a FILE_FOUND message is received, it requests the download of the specified file.
        If a FILE_NOT_FOUND message is received, it prints a message indicating that the file was not found.
        If an invalid message is received, it prints a message indicating that the message is invalid.
        """
        data = ""
        while not self.exit:
            chunk = self.tcp_socket.recv(1024).decode('utf-8')
            data += chunk

            if '<' in data:
                messages = data.split('<')
                for message in messages:
                    if message:
                        threading.Thread(target=self.handle_tracker_message, args=(message,), daemon=True).start()
                data = ""

            if not chunk:
                break

    def handle_tracker_message(self, message):
        if message.startswith("FILE_FOUND"):
            filename = self.request_download(message)

        elif message.startswith("FILE_NOT_FOUND"):
            _, filename = message.split(" ", 1)
            print(f"File '{filename}' not found in the network.")

        elif message.startswith("B_FOUND"):
            self.register_blocks(message)

        elif message.startswith("B_NOT_FOUND"):
            _, filename = message.split(' ', 1)
            print(f"Individual blocks of the file {filename} not found in the network.")

        elif message.startswith("ALREADY_FILE"):
            _, filename = message.split(' ', 1)
            print(f"File {filename} already exists.")

        else:
            print("Invalid Message.")

    def register_blocks(self, message):
        """
        Registers the blocks of a file.

        Args:
            message (str): The message containing the file information and blocks.

        Returns:
            None
        """
        _, info = message.split(" ", 1)
        file_and_blocks = info.split("~")
        filename = file_and_blocks[0]
        blocks = file_and_blocks[1].split(";")

        for block in blocks:
            node, block_id = block.split(",")
            if (node, filename) not in self.node_blocks:
                self.node_blocks[(node, filename)] = {block_id}
            else:
                self.node_blocks[(node, filename)].add(block_id)

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
            if self.nodes_responsetime[node] < self.nodes_responsetime[fastest_node]:
                fastest_node = node

        self.nodes_responsetime.clear()

        return fastest_node

    def handle_node_chunks(self):
        """
        Handles incoming messages from other nodes.

        Receives messages over UDP socket and processes them, in another thread, based on their content.
        The messages can be of different types, such as DOWNLOAD_REQUEST, DOWNLOAD_RESPONSE,
        PING, PRESPONSE, or Invalid Message.

        Returns:
            None
        """
        data = ""
        while not self.exit:
            chunk, sender_address = self.udp_socket.recvfrom(1024)
            data += chunk.decode('utf-8')

            if '<' in data:
                messages = data.split('<')
                for message in messages:
                    if message:
                        node_name = socket.gethostbyaddr(sender_address[0])[0]
                        threading.Thread(target=self.handle_node_message, args=(message, node_name), daemon=True).start()

                data = ""

            if not chunk:
                break

    def handle_node_message(self, message, node_name):
        if message.startswith("DOWNLOAD_REQUEST"):
            _, filename = message.split(',')
            self.send_file_blocks(filename, node_name)

        elif message.startswith("BLOCK"):
            _, filename, block_number, total_blocks, checksum, block_content = message.split('~')
            if self.verify_block_checksum(filename, block_number, total_blocks, checksum, block_content, node_name):
                self.blocks[(filename, int(block_number))] = block_content
                self.send_tracker_message(f"GOT_BLOCK,{filename},{block_number}")

            if block_number == total_blocks:
                file_content = self.collect_file_blocks(filename, int(total_blocks))
                self.send_tracker_message(f"DONE,{filename}")
                self.write_file(filename, file_content, node_name)
        
        elif message.startswith("CORRUPTED_BLOCK"):
            _, filename, block_number, total_blocks = message.split(',')
            self.send_file_block(node_name, filename, block_number, total_blocks, checksum, block_content)
        
        elif message.startswith("PING"):
            _, start_time = message.split(';')
            self.send_presponse(start_time, node_name)

            print(f"Ping response sent to {node_name}")

        elif message.startswith("PRESPONSE"):
            _, start_time = message.split(';')
            self.set_response_time(float(start_time), node_name)

        else:
            print("Invalid Message.")

    def calculate_total_blocks(self, file_path):
        """
        Calculates the total number of blocks of a file.

        Args:
            file_path (str): The path to the file.

        Returns:
            int: Total number of blocks of the file.
        """
        file_size = os.path.getsize(file_path)
        total_blocks = (file_size + self.MAX_CHAR_BLOCK - 1) // self.MAX_CHAR_BLOCK
        return total_blocks

    def calculate_checksum(self, block_content):
        """
        Calculates the checksum of a block.

        Args:
            block_content (str): The content of the block.

        Returns:
            str: The checksum of the block.
        """
        sha256 = hashlib.sha256()
        sha256.update(block_content)
        checksum = sha256.hexdigest()
        return checksum

    def send_file_blocks(self, filename, node_name):
        file_path = os.path.join(self.files_folder, filename)
        total_blocks = self.calculate_total_blocks(file_path)

        with open(file_path, 'rb') as file:
            block_number = 1
            while True:
                block_content = file.read(self.MAX_CHAR_BLOCK)
                if not block_content:
                    break

                checksum = self.calculate_checksum(block_content)
                self.current_sending_blocks[(filename, block_number)] = (block_content, checksum)

                message = f"BLOCK~{filename}~{block_number}~{total_blocks}~{checksum}~{block_content.decode('utf-8')}"
                self.send_node_message(message, node_name)
                print(f"Block {block_number}/{total_blocks} of file {filename} sent to {node_name}")

                block_number += 1

        print(f"All blocks of file {filename} sent to {node_name}")

    def send_file_block(self, node_name, filename, block_number, total_blocks):
        block_content = self.current_sending_blocks[(filename, int(block_number))][0]
        checksum = self.current_sending_blocks[(filename, int(block_number))][1]

        message = f"BLOCK~{filename}~{block_number}~{total_blocks}~{checksum}~{block_content.decode('utf-8')}"
        self.send_node_message(message, node_name)

        print(f"Block {block_number}/{total_blocks} of file {filename} sent again to {node_name}")

    def verify_block_checksum(self, filename, block_number, total_blocks, expected_checksum, received_content, node_name):
        calculated_checksum = self.calculate_checksum(received_content.encode('utf-8'))

        if calculated_checksum == expected_checksum:
            print(f"Block {block_number}/{total_blocks} of file {filename} verified successfully.")
            return True
        else:
            print(f"Block {block_number}/{total_blocks} of file {filename} is corrupted, trying again!")
            self.send_node_message(f"CORRUPTED_BLOCK,{filename},{block_number},{total_blocks}", node_name)
            return False
        
    def collect_file_blocks(self, filename, total_blocks):
        """
        Collects all blocks of a file from the network.

        Args:
            filename (str): The name of the file to be collected.
            total_blocks (int): The total number of blocks of the file.

        Returns:
            str: The content of the file.
        """
        file_content = ""
        for block_number in range(1, total_blocks + 1):
            while (filename, block_number) not in self.blocks:
                time.sleep(0.1)
            file_content += self.blocks[(filename, block_number)]
            del self.blocks[(filename, block_number)]
        return file_content

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
        with open(f"{self.files_folder}/{filename}", 'wb') as file:
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
        while not self.exit:
            user_input = input("Enter command (e.g., 'GET <filename>' or 'EXIT' to quit): \n")
            if user_input.startswith("GET"):
                filename = user_input[4:]
                self.send_tracker_message(f"GET,{filename}")

            elif user_input.upper() == "EXIT":
                self.exit = True
                self.send_tracker_message("EXIT")
                self.udp_socket.close()

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