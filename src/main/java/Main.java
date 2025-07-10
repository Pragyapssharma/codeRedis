import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
    public static void main(String[] args) {
        System.out.println("Logs from your program will appear here!");

        // Initialize variables
        ServerSocket serverSocket = null;
        int port = 6379;
        String masterHost = null;
        int masterPort = 0;
        
     // Parse command-line arguments
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) 
            {
                case "--dir":
                	if (i + 1 < args.length) {
                        Config.dir = args[i + 1];
                        i++;
                    } else {
                        System.err.println("Missing value for --dir");
                        return;
                    }
                    break;
                    
                case "--dbfilename":
                	if (i + 1 < args.length) {
                        Config.dbFilename = args[i + 1];
                        i++;
                    } else {
                        System.err.println("Missing value for --dbfilename");
                        return;
                    }
                    break;
                    
                case "--port":
                	if (i + 1 < args.length) {
                        try {
                            port = Integer.parseInt(args[i + 1]);
                            Config.port = port;
                            i++;
                        } catch (NumberFormatException e) {
                            System.err.println("Invalid port number: " + args[i + 1]);
                            return;
                        }
                    } else {
                        System.err.println("Missing value for --port");
                        return;
                    }
                    break;
                    
                case "--replicaof":
                	if (i + 2 < args.length) {
                        Config.isReplica = true;
                        masterHost = args[i + 1];
                        try {
                            masterPort = Integer.parseInt(args[i + 2]);
                        } catch (NumberFormatException e) {
                            System.err.println("Invalid replica port number: " + args[i + 2]);
                            return;
                        }
                        i += 2;
                    } else {
                        System.err.println("Usage: --replicaof <host> <port>");
                        return;
                    }
                    break;   
                    
                default:
                    break;
            }
        }

        // Show parsed config (for debug/logging purposes)
        System.out.println("Configured dir: " + Config.dir);
        System.out.println("Configured dbfilename: " + Config.dbFilename);
        System.out.println("Configured port: " + Config.port);
        System.out.println("Replica mode: " + Config.isReplica);
        
        
        
        if (!Config.dir.isEmpty() && !Config.dbFilename.isEmpty()) {
            String filePath = Config.dir + "/" + Config.dbFilename;
            File rdbFile = new File(filePath);
            if (rdbFile.exists() && rdbFile.isFile()) {
                try (FileInputStream fis = new FileInputStream(rdbFile)) {
                    RDBParser.loadFromStream(fis);
                    System.out.println("Loaded RDB file: " + filePath);
                } catch (IOException e) {
                    System.out.println("Failed to read RDB: " + e.getMessage());
                    e.printStackTrace();
                }
            } else {
                System.out.println("No RDB file found, starting with empty DB");
            }
        } else {
            System.out.println("No RDB config provided, starting with empty DB");
        }
        
        
     // If replica mode, connect to master and send PING
        if (Config.isReplica) {
            try {
                System.out.println("Connecting to master at " + masterHost + ":" + masterPort);
                Socket masterSocket = new Socket(masterHost, masterPort);
                OutputStream out = masterSocket.getOutputStream();

                // Send PING command as RESP array
                String pingCmd = "*1\r\n$4\r\nPING\r\n";
                out.write(pingCmd.getBytes("UTF-8"));
                out.flush();

                System.out.println("Sent PING to master");

                // You may want to read master's response here, or continue handshake in separate logic
                // For now just keep connection open or close after handshake as per your design

            } catch (IOException e) {
                System.err.println("Failed to connect/send PING to master: " + e.getMessage());
                e.printStackTrace();
                return;
            }
        }
        
        // Start server on specified port
        try {
            // Set up the server socket and wait for client connections
        	serverSocket = new ServerSocket();
        	serverSocket.setReuseAddress(true);
        	serverSocket.bind(new InetSocketAddress(Config.port));
            System.out.println("Server is listening on port " + port);

            // Infinite loop to accept multiple clients concurrently
            while (true) {
                // Wait for a client connection
                Socket clientSocket = serverSocket.accept();
                System.out.println("Client connected.");

                // Spawn a new thread to handle the client
                new ClientHandler(clientSocket).start();
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        } finally {
            try {
                if (serverSocket != null) {
                    serverSocket.close();
                }
            } catch (IOException e) {
                System.out.println("IOException during cleanup: " + e.getMessage());
            }
        }
    }
}