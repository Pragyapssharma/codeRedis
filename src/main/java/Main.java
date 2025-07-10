import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
    public static void main(String[] args) {
        System.out.println("Logs from your program will appear here!");

        // Initialize variables
        ServerSocket serverSocket = null;
        int port = 6379;
        
     // Parse command-line arguments
        for (int i = 0; i < args.length - 1; i++) {
            switch (args[i]) 
            {
                case "--dir":
                    Config.dir = args[i + 1];
                    break;
                    
                case "--dbfilename":
                    Config.dbFilename = args[i + 1];
                    break;
                    
                case "--port":
                    try {
                        port = Integer.parseInt(args[i + 1]);
                        Config.port = port;
                    } catch (NumberFormatException e) {
                        System.err.println("Invalid port number: " + args[i + 1]);
                        return;
                    }
                    break;
                    
                case "--replicaof":
                    Config.isReplica = true;
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
        
        
//        String filePath = Config.dir + "/" + Config.dbFilename;
//        File rdbFile = new File(filePath);
        
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