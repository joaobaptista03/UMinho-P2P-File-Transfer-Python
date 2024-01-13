# Distributed File Sharing Network
This project is a P2P (peer-to-peer) app inspired in BitTorrent, it uses TCP / UDP connections to send messages between the Tracker and the Nodes, sending the messages directly from Node to Node.

## FSNode
The `FSNode` class represents a file system node in the network. It handles file storage, communication with other nodes, and interaction with the tracker server.

## FSTracker
The FSTracker class acts as a central tracker in the network. It keeps track of all nodes and the files they store.

## Usage
To run the network, start the FSTracker on a central server, and then start FSNode instances on the participating nodes in the network. Ensure that each FSNode is configured with the correct tracker information, modify the zones files according to the IPs you want to include in the DNS Server.

