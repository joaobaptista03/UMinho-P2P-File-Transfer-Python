import socket
import sys
import threading

class FSTracker:
    def __init__(self, tracker_name, ip, port):
        """
        Initialize a new FSTracker object.

        Args:
            tracker_name (str): The name of the tracker.
            ip (str): The IP address of the tracker.
            port (int): The port number of the tracker.
        """
        self.name = tracker_name
        self.ip = ip
        self.port = port

        self.tcp_socket = None

        self.node_files = {}
        self.ip_to_node_name = {}

    def start(self):
        """
        Starts the tracker and listens for incoming connections from nodes.
        """
        self.tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_socket.bind((self.ip, self.port))
        self.tcp_socket.listen(5)

        print(f"{self.name} listening on {self.ip}:{self.port}")

        while True:
            node_socket, node_address = self.tcp_socket.accept()
            node_thread = threading.Thread(target = self.handle_node_message, args = (node_socket, node_address))
            node_thread.start()

    def handle_node_message(self, node_socket, node_address):
        """
        Handles the messages received from a node.

        Args:
            node_socket (socket): The socket object for communication with the node.
            node_address (tuple): The address of the node.

        Returns:
            None
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
                        if node_address[0] in self.ip_to_node_name:
                            print(f"Received message from {self.ip_to_node_name[node_address[0]]}: {message}")

                        if message.startswith("REGISTER") and node_address[0] not in self.ip_to_node_name:
                            _, node_name, files = message.split(',')
                            self.register_node(files, node_address, node_name, node_socket)

                            print(f"Received Node register message from {self.ip_to_node_name[node_address[0]]}: {message}")
                            print(f"Node \"{node_name}\" registered ({node_address[0]}:{node_address[1]}) with the files: {files}")

                        elif message.startswith("GET"):
                            filename = message[4:]
                            self.send_nodes_to_node(filename, node_address, node_socket)

                            print(f"Nodes that contain the file {filename} sent to {self.ip_to_node_name[node_address[0]]}")

                        elif message.startswith("EXIT"):
                            if node_address[0] in self.node_files:
                                del self.node_files[node_address[0]]
                            del self.ip_to_node_name[node_address[0]]
                            node_socket.close()
                            exitFlag = True

                            print("Node " + self.ip_to_node_name[node_address[0]] + " exited.")

                        else:
                            print("Invalid Message.")

                data = ""

            if not chunk:
                break

    def register_node(self, files, node_address, node_name, node_socket):
        """
        Registers a node with the given files.

        Args:
            files (str): A string containing the files separated by ';'.
            node_address (tuple): The address of the node.

        Returns:
            None
        """
        self.ip_to_node_name[node_address[0]] = node_name
        node_socket.send(f"REGISTERED,{self.name}<".encode('utf-8'))

        if len(files) > 0:
            files_list = files.split(';')
            self.node_files[node_address[0]] = set(files_list)

        

    def send_nodes_to_node(self, filename, node_address, node_socket):
        """
        Sends the list of nodes that have the specified file to the requesting node.

        Args:
            filename (str): The name of the file to search for.
            node_address (tuple): The address of the requesting node.
            node_socket (socket): The socket object for communication with the requesting node.

        Returns:
            None
        """
        nodes_with_file = [(node, files) for node, files in self.node_files.items() if filename in files and node_address[0] != node]
        
        if nodes_with_file:
            nodeResult = ""
            for (node, _) in nodes_with_file:
                nodeResult += node + ";"
            nodeResult = nodeResult[:-1]
            
            response = f"FILE_FOUND {filename}~{nodeResult}<"
            node_socket.send(response.encode('utf-8'))

            self.update_node_files(node_address, filename)

        else:
            response = f"FILE_NOT_FOUND {filename}<"
            node_socket.send(response.encode('utf-8'))

    def update_node_files(self, node_address, filename):
        """
        Update the files associated with a node node.

        Args:
            node_address (tuple): The address of the node node.
            filename (str): The name of the file to be added.

        Returns:
            None
        """
        if node_address[0] in self.node_files:
            self.node_files[node_address[0]].add(filename)
        else:
            self.node_files[node_address[0]] = {filename}

if __name__ == "__main__":
    args = sys.argv[1:]

    tracker_name = args[2]
    ip = args[0]
    port = int(args[1])


    tracker = FSTracker(tracker_name, ip, port)
    tracker.start()