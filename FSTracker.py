import socket
import sys
import threading

class FSTracker:
    """
    FSTracker class represents a file system tracker that keeps track of nodes and their files.

    Attributes:
        name (str): The name of the tracker.
        port (int): The port number on which the tracker listens for connections.
        tcp_socket (socket.socket): The TCP socket used for communication.
        node_files (dict): A dictionary that maps node names to the set of files they have.
        node_blocks (dict): A dictionary that maps (node_name, filename) to blocks of the file that the node has.
        files_lock (threading.Lock): A lock used for thread synchronization in node_files dictionary.
        blocks_lock (threading.Lock): A lock used for thread synchronization in node_blocks dictionary.
        exit_flag_nodes (lsit): A list of nodes that asked to exit.

        TODO's:
        - 
    """
    
    def __init__(self, tracker_name, port):
        self.name = tracker_name
        self.port = port
        self.tcp_socket = None

        self.node_files = {}
        self.node_blocks = {}

        self.files_lock = threading.Lock()
        self.blocks_lock = threading.Lock()

        self.exit_flag_nodes = []

    def start(self):
        """
        Starts the tracker by creating a TCP socket, binding it to the specified address and port,
        and listening for incoming connections. For each incoming connection, a new thread is created
        to handle the node's messages.
        """
        self.tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_socket.bind((self.name, self.port))
        self.tcp_socket.listen(5)

        print(f"{self.name} listening on port {self.port}")

        while True:
            node_socket, node_address = self.tcp_socket.accept()
            node_name = socket.gethostbyaddr(node_address[0])[0]
            node_thread = threading.Thread(target=self.handle_node_chunks, args=(node_socket, node_name))
            node_thread.start()

    def handle_node_chunks(self, node_socket, node_name):
        """
        Handles the chunks received from a node socket.
        It uses threading to handle each message in a separate thread.

        Args:
            node_socket (socket.socket): The socket object for the node.
            node_name (str): The name of the node.

        Returns:
            None
        """
        data = ""
        while node_name not in self.exit_flag_nodes:
            chunk = node_socket.recv(1024).decode('utf-8')
            data += chunk

            if '<' in data:
                messages = data.split('<')
                for message in messages:
                    if message:
                        threading.Thread(target=self.handle_node_message, args=(message, node_name, node_socket)).start()

                data = ""

            if not chunk:
                break
        
        self.exit_flag_nodes.remove(node_name)

    def handle_node_message(self, message, node_name, node_socket):
        """
        Handles the messages received from a node.

        Args:
            message (str): The message received from the node.
            node_name (str): The name of the node.
            node_socket (socket): The socket connection to the node.

        If the message starts with "EXIT", the node is removed from the tracker.
        If the message starts with "REGISTER", the node is registered with the tracker.
        If the message starts with "GET", the nodes that contain the file are sent to the node.
        If the message starts with "GOT_BLOCK", the block is added to the node's blocks.
        If the message starts with "DONE", the node is updated with the file it received.

        Returns:
            None
        """
        if message.startswith("EXIT"):
            self.exit_flag_nodes.append(node_name)
            with self.files_lock:
                if node_name in self.node_files:
                    del self.node_files[node_name]
            node_socket.close()
            print("Node " + node_name + " exited.")
        
        elif message.startswith("REGISTER"):
            _, files = message.split(',')
            self.register_node(files, node_name)
            print(f"Node \"{node_name}\" registered with the files: {files}")

        elif message.startswith("GET"):
            filename = message[4:]
            self.send_nodes_to_node(filename, node_name, node_socket)

        elif message.startswith("GOT_BLOCK"):
            _, filename, block_id = message.split(',')
            with self.blocks_lock:
                if filename in self.node_blocks:
                    self.node_blocks[(node_name, filename)].add(block_id)
                else:
                    self.node_blocks[(node_name, filename)] = {block_id}
            print(f"Node {node_name} has block {block_id} of file {filename}")

        elif message.startswith("DONE"):
            _, filename = message.split(',')
            with self.blocks_lock:
                del self.node_blocks[(node_name, filename)]
            self.update_node_files(node_name, filename)
            print(f"Node {node_name} has finished downloading file {filename}")

        else:
            print("Invalid Message.")

    def register_node(self, files, node_name):
        """
        Registers a node with the tracker and updates its file list.

        Args:
            files (str): A string containing the files of the node, separated by ';'.
            node_name (str): The name of the node.
        """
        if len(files) > 0:
            files_list = files.split(';')
            with self.files_lock:
                self.node_files[node_name] = set(files_list)

    def send_nodes_to_node(self, filename, node_name, node_socket):
        """
        Sends the list of nodes that contain a specific file to a node.

        Args:
            filename (str): The name of the file.
            node_name (str): The name of the node.
            node_socket (socket.socket): The socket used for communication with the node.
        """
        nodes_with_file = [node for node, files in self.node_files.items() if filename in files]
        nodes_with_blocks = [(node, blocks) for (node, file), blocks in self.node_blocks.items() if filename == file]

        if nodes_with_file:
            node_ip_result = ""
            for node in nodes_with_file:
                if node == node_name:
                    response = f"ALREADY_FILE {filename}<"
                    node_socket.send(response.encode('utf-8'))
                    print(f"File {filename} already exists in node {node_name}")
                    return
                node_ip_result += node + ";"
            node_ip_result = node_ip_result[:-1]
            
            response = f"FILE_FOUND {filename}~{node_ip_result}<"
            node_socket.send(response.encode('utf-8'))
            print(f"Nodes that contain the file {filename} sent to {node_name}")

        else:
            response = f"FILE_NOT_FOUND {filename}<"
            node_socket.send(response.encode('utf-8'))
            print(f"File {filename} wasn't found in any node")

        if nodes_with_blocks:
            node_ip_result = ""
            for node, blocks in nodes_with_blocks:
                for block in blocks:
                    node_ip_result += f"{node},{block};"
            node_ip_result = node_ip_result[:-1]

            response = f"B_FOUND {filename}~{node_ip_result}<"
            node_socket.send(response.encode('utf-8'))
            print(f"Nodes that contain blocks of the file {filename} sent to {node_name}")
        
        else:
            response = f"B_NOT_FOUND {filename}<"
            node_socket.send(response.encode('utf-8'))
            print(f"Blocks of the file {filename} wasn't found in any node")

    def update_node_files(self, node_name, filename):
        """
        Updates the file list of a node after it has received a file.

        Args:
            node_name (str): The name of the node.
            filename (str): The name of the file.
        """
        with self.files_lock:
            if node_name in self.node_files:
                self.node_files[node_name].add(filename)
            else:
                self.node_files[node_name] = {filename}

if __name__ == "__main__":
    args = sys.argv[1:]

    tracker_name = args[0]
    port = int(args[1])

    tracker = FSTracker(tracker_name, port)
    tracker.start()