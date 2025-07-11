import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
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
                	if (i + 1 < args.length) {
                        Config.isReplica = true;
                        String[] parts = args[i + 1].split(" ");
                        if (parts.length == 2) {
                            masterHost = parts[0];
                            try {
                                masterPort = Integer.parseInt(parts[1]);
                            } catch (NumberFormatException e) {
                                System.err.println("Invalid replica port number: " + parts[1]);
                                return;
                            }
                            i++;
                        } else {
                            System.err.println("Usage: --replicaof <host> <port>");
                            return;
                        }
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
                InputStream in = masterSocket.getInputStream();

                // Send PING command as RESP array
                String pingCmd = "*1\r\n$4\r\nPING\r\n";
                out.write(pingCmd.getBytes("UTF-8"));
                out.flush();

                System.out.println("Sent PING to master");

             // Read PING response (+PONG\r\n expected)
                String pongResp = readLine(in);
                System.out.println("Received from master: " + pongResp);
                if (!pongResp.equals("+PONG")) {
                    System.err.println("Unexpected response to PING: " + pongResp);
                    masterSocket.close();
                    return;
                }

                // Send first REPLCONF: listening-port <port>
                String portStr = Integer.toString(port);
                String replconfListeningPort = String.format(
                    "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%d\r\n%s\r\n",
                    portStr.length(), portStr);
                out.write(replconfListeningPort.getBytes("UTF-8"));
                out.flush();
                System.out.println("Sent REPLCONF listening-port " + portStr);

                // Read response to first REPLCONF (+OK\r\n expected)
                String replconfResp1 = readLine(in);
                System.out.println("Received from master: " + replconfResp1);
                if (!replconfResp1.equals("+OK")) {
                    System.err.println("Unexpected response to REPLCONF listening-port: " + replconfResp1);
                    masterSocket.close();
                    return;
                }

                // Send second REPLCONF: capa psync2
                String replconfCapa = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
                out.write(replconfCapa.getBytes("UTF-8"));
                out.flush();
                System.out.println("Sent REPLCONF capa psync2");

                // Read response to second REPLCONF (+OK\r\n expected)
                String replconfResp2 = readLine(in);
                System.out.println("Received from master: " + replconfResp2);
                if (!replconfResp2.equals("+OK")) {
                    System.err.println("Unexpected response to REPLCONF capa: " + replconfResp2);
                    masterSocket.close();
                    return;
                }
                
             // Send PSYNC ? -1
                String psyncCmd = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
                out.write(psyncCmd.getBytes("UTF-8"));
                out.flush();
                System.out.println("Sent PSYNC ? -1");
                String psyncResp = readLine(in);
                System.out.println("Received: " + psyncResp);
                
             // Start a new thread to read propagated commands from master
                new Thread(() -> {
                    try {
                        byte[] buffer = new byte[1024];
                        int bytesRead;
                        ByteArrayOutputStream baos = new ByteArrayOutputStream();

                        while ((bytesRead = in.read(buffer)) != -1) {
                            baos.write(buffer, 0, bytesRead);
                            byte[] data = baos.toByteArray();
                            int processed = processPropagatedCommands(data);
                            if (processed > 0) {
                                baos.reset();
                                baos.write(data, processed, data.length - processed);
                            }
                        }
                    } catch (IOException e) {
                        System.err.println("Failed to read propagated commands: " + e.getMessage());
                        e.printStackTrace();
                    }
                }).start();  

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
    
    
    private static String readLine(InputStream in) throws IOException {
        StringBuilder sb = new StringBuilder();
        int b;
        boolean gotCR = false;
        while ((b = in.read()) != -1) {
            if (gotCR) {
                if (b == '\n') {
                    break; // End of line
                } else {
                    sb.append('\r');
                    gotCR = false;
                }
            }
            if (b == '\r') {
                gotCR = true;
            } else {
                sb.append((char) b);
            }
        }
        return sb.toString();
    }

    private static int processPropagatedCommands(byte[] data) throws IOException {
        int processed = 0;
        RespParser parser = new RespParser(data);

        while (parser.hasNext()) {
            RespCommand command = parser.next();
            if (command != null) {
                processCommand(command);
                processed += command.getRaw().length;
            } else {
                break;
            }
        }

        return processed;
    }

    // Method to process a command
    private static void processCommand(RespCommand command) {
        if (command.getType() == RespCommand.Type.ARRAY) {
            String[] elements = command.getArray();
            if (elements[0].equalsIgnoreCase("SET")) {
                ClientHandler.handleSet(elements, null); // null OutputStream => no reply
            }
            // Add more command types as needed
        }
    }
    
}