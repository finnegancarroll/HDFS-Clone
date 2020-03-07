SUFS CLIENT PROGRAM

Design Choices:
-Splitting of files is done on the client
-Client does not contact namenode for block placement
-Blocks are replicated on every available node, i.e. replication factor = datanode count