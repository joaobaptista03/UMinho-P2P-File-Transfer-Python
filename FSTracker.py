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
        lock (threading.Lock): A lock used for thread synchronization.
    """
    
    def __init__(self, tracker_name, port):
        self.name = tracker_name
        self.port = port
        self.tcp_socket = None
        self.node_files = {}
        self.lock = threading.Lock()

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
            node_thread = threading.Thread(target=self.handle_node_message, args=(node_socket, node_name))
            node_thread.start()

    def handle_node_message(self, node_socket, node_name):
        """
        Handles the messages received from a node.

        Args:
            node_socket (socket.socket): The socket used for communication with the node.
            node_name (str): The name of the node.

        """
        data = ""
        exitFlag = False
        while exitFlag == False:
            chunk = node_socket.recv(1024).decode('utf-8')
            data += chunk

            if '<' in data:
                messages = data.split('<')
                for message in messages:
                    if message:
                        if message.startswith("REGISTER"):
                            _, files = message.split(',')
                            self.register_node(files, node_name)
                            print(f"Node \"{node_name}\" registered with the files: {files}")

                        elif message.startswith("GET"):
                            filename = message[4:]
                            self.send_nodes_to_node(filename, node_name, node_socket)
                            print(f"Nodes that contain the file {filename} sent to {node_name}")

                        elif message.startswith("EXIT"):
                            with self.lock:
                                if node_name in self.node_files:
                                    del self.node_files[node_name]
                            node_socket.close()
                            exitFlag = True

                            print("Node " + node_name + " exited.")

                        else:
                            print("Invalid Message.")

                data = ""

            if not chunk:
                break

    def register_node(self, files, node_name):
        """
        Registers a node with the tracker and updates its file list.

        Args:
            files (str): A string containing the files of the node, separated by ';'.
            node_name (str): The name of the node.
        """
        if len(files) > 0:
            files_list = files.split(';')
            with self.lock:
                self.node_files[node_name] = set(files_list)

    def send_nodes_to_node(self, filename, node_name, node_socket):
        """
        Sends the list of nodes that contain a specific file to a node.

        Args:
            filename (str): The name of the file.
            node_name (str): The name of the node.
            node_socket (socket.socket): The socket used for communication with the node.
        """
        with self.lock:
            nodes_with_file = [node for node, files in self.node_files.items() if filename in files and node_name != node]
        
        if nodes_with_file:
            node_ip_result = ""
            for node in nodes_with_file:
                node_ip_result += node + ";"
            node_ip_result = node_ip_result[:-1]
            
            response = f"FILE_FOUND {filename}~{node_ip_result}<"
            node_socket.send(response.encode('utf-8'))

            self.update_node_files(node_name, filename)

        else:
            response = f"FILE_NOT_FOUND {filename}<"
            node_socket.send(response.encode('utf-8'))

    def update_node_files(self, node_name, filename):
        """
        Updates the file list of a node after it has received a file.

        Args:
            node_name (str): The name of the node.
            filename (str): The name of the file.
        """
        with self.lock:
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
