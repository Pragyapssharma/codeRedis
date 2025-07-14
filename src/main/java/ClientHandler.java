import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

class ClientHandler extends Thread {
    private Socket clientSocket;
    private InputStream in;
    private OutputStream out;
    private static final Map<String, KeyValue> keyValueStore = new HashMap<>();
    private static final List<OutputStream> replicaOutputs = new CopyOnWriteArrayList<>();
    private static final byte[] EMPTY_RDB_FILE = new byte[] {
    	    (byte) 0x52, (byte) 0x45, (byte) 0x44, (byte) 0x49, // REDI
    	    (byte) 0x53, (byte) 0x30, (byte) 0x30, (byte) 0x30, (byte) 0x39, // S0009
    	    (byte) 0xFA, (byte) 0x00, // auxiliary field with 0 length
    	    (byte) 0xFF,             // end-of-RDB opcode
    	    (byte) 0x00, (byte) 0x00, // dummy checksum
    	    (byte) 0x00, (byte) 0x00  // padding (some implementations expect 18 bytes total)
    	};

    private boolean isReplicaConnection = false;

    public ClientHandler(Socket clientSocket) {
        this.clientSocket = clientSocket;
    }
    
    public static void putKeyWithExpiry(String key, String value, long expirationUnixMs) {
        keyValueStore.put(key, new KeyValue(value, expirationUnixMs));
    }

    @Override
    public void run() {
        try (
            BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            OutputStream out = clientSocket.getOutputStream()
        ) {
        	this.out = out;
        	if (Config.isReplica) {
                processMasterHandshake(in, out);
            }
            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                System.out.println("Received: " + inputLine);

                if (inputLine.startsWith("*")) {
                    int argCount = Integer.parseInt(inputLine.substring(1));
                    List<String> args = readArguments(in, argCount);

                    if (args.isEmpty()) continue;

                    String command = args.get(0).toUpperCase();
                    
                    if (Config.isReplica) {
                        // Process replicated command silently
                        switch (command) {
                            case "SET":
                                handleSet(args, null); // null OutputStream => no reply
                                break;
                            default:
                                System.out.println("Replica received unsupported command: " + command);
                                break;
                        }
                        continue;
                    }
                    
                    switch (command) {
                        case "PING":
                            out.write("+PONG\r\n".getBytes());
                            break;
                            
                        case "REPLCONF":
                            out.write("+OK\r\n".getBytes());
                            break;
                            
                        case "PSYNC":
//                        	if (args.size() == 3 && args.get(1).equals("?") && args.get(2).equals("-1")) {
//                                String replId = Config.masterReplId;
//                                String fullResync = "+FULLRESYNC " + replId + " 0\r\n";
//                                out.write(fullResync.getBytes());
//
//                                // Send empty RDB
//                                byte[] rdbBytes = EMPTY_RDB_FILE;
//                                String header = "$" + rdbBytes.length + "\r\n";
//                                out.write(header.getBytes());
//                                out.write(rdbBytes);
//                                System.out.println("Sent FULLRESYNC and empty RDB file (" + rdbBytes.length + " bytes)");
//
//                                // Mark this as replica
//                                isReplicaConnection = true;
//                                replicaOutputs.add(out);
//                                System.out.println("Added new replica connection. Total: " + replicaOutputs.size());
//                            } else {
//                                out.write("-ERR unsupported PSYNC format\r\n".getBytes());
//                            }
                        	handlePsync(args, out);
                            break;


                        case "ECHO":
                            if (args.size() >= 2) {
                                String echo = args.get(1);
                                String response = "$" + echo.length() + "\r\n" + echo + "\r\n";
                                out.write(response.getBytes());
                            }
                            break;

                        case "SET":
                            handleSet(args, out);
                            break;

                        case "GET":
                            handleGet(args, out);
                            break;
                        
                        case "CONFIG":
                            handleConfig(args, out);
                            break;
                            
                        case "KEYS":
                            handleKeys(args, out);
                            break;
                            
                        case "INFO":
                            handleInfo(args, out);
                            break;

                        default:
                            System.out.println("Unknown command: " + command);
                            out.write(("-ERR unknown command\r\n").getBytes());
                            break;
                    }
                } else {
                    System.out.println("Invalid command format: " + inputLine);
                }
            }
        } catch (IOException e) {
            System.out.println("IOException handling client: " + e.getMessage());
        } finally {
            try {
                if (clientSocket != null) {
                    clientSocket.close();
                }
            } catch (IOException e) {
                System.out.println("IOException during client socket cleanup: " + e.getMessage());
            } finally {
            	if (isReplicaConnection) {
                    replicaOutputs.remove(out);
                    System.out.println("Replica removed. Remaining: " + replicaOutputs.size());
                }
            }
        }
    }

    private List<String> readArguments(BufferedReader in, int count) throws IOException {
        List<String> args = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            String lenLine = in.readLine(); // e.g., $5
            if (lenLine == null || !lenLine.startsWith("$")) {
                throw new IOException("Expected bulk string, got: " + lenLine);
            }

            String arg = in.readLine(); // actual content
            args.add(arg);
        }
        return args;
    }

    public static void handleSet(List<String> args, OutputStream out) throws IOException {
        if (args.size() < 3) {
            out.write(("-ERR wrong number of arguments for 'SET'\r\n").getBytes());
            return;
        }

        String key = args.get(1);
        String value = args.get(2);

        long expiryMillis = 0;
        if (args.size() >= 5 && args.get(3).equalsIgnoreCase("px")) {
            try {
                expiryMillis = Long.parseLong(args.get(4));
            } catch (NumberFormatException e) {
            	if (out != null) {
                    out.write(("-ERR PX value is not a number\r\n").getBytes());
                }
                return;
            }
        }

        long expirationTimestamp = expiryMillis > 0 ? System.currentTimeMillis() + expiryMillis : 0;
        keyValueStore.put(key, new KeyValue(value, expirationTimestamp));

        if (out != null) {
            out.write("+OK\r\n".getBytes());
        }
        
     // Propagate to all replicas
      if (out != null) {
        StringBuilder command = new StringBuilder();
        command.append("*3\r\n");
        command.append("$3\r\nSET\r\n");
        command.append("$").append(key.length()).append("\r\n").append(key).append("\r\n");
        command.append("$").append(value.length()).append("\r\n").append(value).append("\r\n");

        byte[] commandBytes = command.toString().getBytes();

        for (OutputStream replicaOut : replicaOutputs) {
            if (replicaOut != out) {
            	try {
                    replicaOut.write(commandBytes); // Propagate the SET command to each replica
                    replicaOut.flush();
                } catch (IOException e) {
                    System.out.println("Replica write failed, removing: " + e.getMessage());
                    replicaOutputs.remove(replicaOut);  // Handle replica disconnection
                }
            }
        }
      }
    }
    
    private void handleGet(List<String> args, OutputStream out) throws IOException {
        if (args.size() != 2) {
            out.write("-ERR wrong number of arguments for 'GET'\r\n".getBytes());
            return;
        }

        String key = args.get(1);
        KeyValue kv = keyValueStore.get(key);

        String response;
        if (kv == null || kv.hasExpired()) {
            System.out.println("GET " + key + " => not found or expired");
            response = RespCommand.bulkString(null); // will return $-1\r\n
        } else {
            response = RespCommand.bulkString(kv.value); // formatted response
        }

        System.out.println("Sending GET response: " + response.replace("\r\n", "\\r\\n"));
        out.write(response.getBytes());
    }
    
//    private void handleGet(List<String> args, OutputStream out) throws IOException {
//        if (args.size() != 2) {
//            out.write("-ERR wrong number of arguments for 'GET'\r\n".getBytes());
//            return;
//        }
//
//        String key = args.get(1);
//        KeyValue kv = keyValueStore.get(key);
//        
//        if (kv == null || kv.hasExpired()) {
//            System.out.println("GET " + key + " => not found or expired");
//            out.write("$-1\r\n".getBytes());  // RESP nil bulk string
//            return;
//        } else {
//        String value = kv.value;  // direct field access, or use kv.getValue() if you add getter
//        System.out.println("Debug : Sending GET response: $" + value.length() + "\\r\\n" + value);
//        out.write(("$" + value.length() + "\r\n" + value + "\r\n").getBytes());
//        }
//    }

    
    public static void handleConfig(List<String> args, OutputStream out) throws IOException {
        if (args.size() < 2) {
            out.write(("-ERR wrong number of arguments for 'CONFIG'\r\n").getBytes());
            return;
        }

        String subCommand = args.get(1).toLowerCase();
        if ("get".equals(subCommand)) {
            if (args.size() < 3) {
                out.write(("-ERR wrong number of arguments for 'CONFIG GET'\r\n").getBytes());
                return;
            }

            String configKey = args.get(2);
            switch (configKey) {
                case "dir":
                    out.write(("$" + Config.dir.length() + "\r\n" + Config.dir + "\r\n").getBytes());
                    break;
                case "dbfilename":
                    out.write(("$" + Config.dbFilename.length() + "\r\n" + Config.dbFilename + "\r\n").getBytes());
                    break;
                case "port":
                    out.write(("$" + String.valueOf(Config.port).length() + "\r\n" + Config.port + "\r\n").getBytes());
                    break;
                case "replica":
                    out.write(("$" + String.valueOf(Config.isReplica).length() + "\r\n" + Config.isReplica + "\r\n").getBytes());
                    break;
                default:
                    out.write(("-ERR unknown configuration parameter\r\n").getBytes());
                    break;
            }
        } else {
            out.write(("-ERR unknown CONFIG subcommand\r\n").getBytes());
        }
    }
    
    public static void handleKeys(List<String> args, OutputStream out) throws IOException {
        StringBuilder keysResponse = new StringBuilder("*" + keyValueStore.size() + "\r\n");
        for (String key : keyValueStore.keySet()) {
            keysResponse.append("$").append(key.length()).append("\r\n").append(key).append("\r\n");
        }
        out.write(keysResponse.toString().getBytes());
    }
    
//    private void handleInfo(List<String> args, OutputStream out) throws IOException {
//        if (args.size() >= 2 && args.get(1).equalsIgnoreCase("replication")) {
//            StringBuilder sb = new StringBuilder();
//
//            sb.append("role:").append(Config.isReplica ? "slave" : "master").append("\r\n");
//            if (!Config.isReplica) {
//                sb.append("master_replid:").append(Config.masterReplId).append("\r\n");
//                sb.append("master_repl_offset:").append(Config.masterReplOffset).append("\r\n");
//            }
//
//            String infoBody = sb.toString();
//            byte[] infoBytes = infoBody.getBytes("UTF-8");
//            String header = "$" + infoBytes.length + "\r\n";
//
//            
//            System.out.println("INFO output (" + infoBytes.length + " bytes):");
//            for (String line : infoBody.split("\r\n")) {
//                System.out.println("> " + line + " [\\r\\n]");
//            }
//            System.out.println("Header to send: " + header.replace("\r", "\\r").replace("\n", "\\n"));
//            System.out.println("Full payload length: " + infoBytes.length);
//            System.out.println("Sending full RESP:\n" + header + infoBody);
//
//            out.write(header.getBytes("UTF-8"));
//            out.write(infoBytes);
//            out.write("\r\n".getBytes("UTF-8"));
//        } else {
//            out.write("-ERR unsupported INFO section\r\n".getBytes());
//        }
//    }
    
    public static void handleInfo(List<String> args, OutputStream out) throws IOException {
        // Simple INFO command response (basic server information)
        String info = "# Server\r\n" +
                "version=1.0\r\n" +
                "uptime=12345\r\n" +
                "# Replication\r\n" +
                "role=" + (Config.isReplica ? "slave" : "master") + "\r\n";
        out.write(("$" + info.length() + "\r\n" + info + "\r\n").getBytes());
    }
    
    public static void handlePsync(List<String> args, OutputStream out) throws IOException {
        if (Config.isReplica) {
            // This server is in replica mode
            if (args.size() < 3) {
                out.write(("-ERR wrong number of arguments for 'PSYNC'\r\n").getBytes());
                return;
            }

            String replicationId = args.get(1);  // The replication ID provided by the master
            long offset = Long.parseLong(args.get(2));  // The replication offset from the master

            // Debugging the received PSYNC request
            System.out.println("Replica received PSYNC: replicationId=" + replicationId + ", offset=" + offset);

            // Responding with FULLRESYNC
            String syncResponse = "+FULLRESYNC " + Config.masterReplId + " " + Config.masterReplOffset + "\r\n";
            out.write(syncResponse.getBytes());  // Send +FULLRESYNC message to the replica

            // Send simulated empty RDB file to the replica
            out.write(EMPTY_RDB_FILE);  // Here, we send an empty RDB file (use actual RDB bytes if needed)

            // Add the current OutputStream to the replica outputs for future propagation of commands
            replicaOutputs.add(out);
            System.out.println("Replica added: " + replicaOutputs.size() + " total replicas connected.");
        } else {
            // If not in replica mode, return an error message
            out.write(("-ERR PSYNC is only valid for replica mode\r\n").getBytes());
        }
    }



    private void processMasterHandshake(BufferedReader in, OutputStream out) throws IOException {
    	
    	// Send PING
        String ping = "*1\r\n$4\r\nPING\r\n";
        out.write(ping.getBytes());

        // Wait for +PONG
        String line = in.readLine();
        if (line != null && line.startsWith("+PONG")) {
            System.out.println("Received PONG from master");
        }
    	
        // Send REPLCONF listening-port
        String replconfPort = "*3\r\n" +
                              "$8\r\nREPLCONF\r\n" +
                              "$14\r\nlistening-port\r\n" +
                              "$" + String.valueOf(Config.port).length() + "\r\n" +
                              Config.port + "\r\n";
        out.write(replconfPort.getBytes());
        
     // Wait for +OK
        line = in.readLine();
        if (line != null && line.startsWith("+OK")) {
            System.out.println("Received OK for REPLCONF listening-port");
        }

        // Send REPLCONF capa psync2
        String replconfCapa = "*3\r\n" +
                              "$8\r\nREPLCONF\r\n" +
                              "$4\r\ncapa\r\n" +
                              "$6\r\npsync2\r\n";
        out.write(replconfCapa.getBytes());
        
     // Wait for +OK
        line = in.readLine();
        if (line != null && line.startsWith("+OK")) {
            System.out.println("Received OK for REPLCONF capa psync2");
        }

        // Send PSYNC ? -1
        String psync = "*3\r\n" +
                       "$5\r\nPSYNC\r\n" +
                       "$1\r\n?\r\n" +
                       "$2\r\n-1\r\n";
        out.write(psync.getBytes());

        out.flush();

     // Wait for +FULLRESYNC
        line = in.readLine();
        if (line != null && line.startsWith("+FULLRESYNC")) {
            System.out.println("Received FULLRESYNC: " + line);
        }

     // Expect RDB bulk string: $<length>
        line = in.readLine();
        if (line != null && line.startsWith("$")) {
            int rdbLength = Integer.parseInt(line.substring(1));
            byte[] rdbBuffer = new byte[rdbLength];
            InputStream inputStream = clientSocket.getInputStream();
            int totalRead = 0;
            while (totalRead < rdbLength) {
                int read = inputStream.read(rdbBuffer, totalRead, rdbLength - totalRead);
                if (read == -1) break;
                totalRead += read;
            }
            inputStream.read();
            inputStream.read();
            System.out.println("Read " + totalRead + " RDB bytes from master.");
            
         // Start separate thread for replication command listening
            new Thread(() -> {
                System.out.println("Start reading replication stream...");
                try {
//                    InputStream inputStream = clientSocket.getInputStream();
                    byte[] buffer = new byte[8192];
                    List<Byte> commandBuffer = new ArrayList<>();

                    int bytesRead;
                    while ((bytesRead = inputStream.read(buffer)) != -1) {
                        for (int i = 0; i < bytesRead; i++) {
                            commandBuffer.add(buffer[i]);
                        }

                        byte[] commandBytes = new byte[commandBuffer.size()];
                        for (int i = 0; i < commandBuffer.size(); i++) {
                            commandBytes[i] = commandBuffer.get(i);
                        }

                        try {
                            RespParser parser = new RespParser(commandBytes);
                            while (parser.hasNext()) {
                                RespCommand cmd = parser.next();
                                if (cmd == null) break;

                                String[] parts = cmd.getArray();
                                String command = parts[0].toUpperCase();

                                if ("SET".equals(command)) {
                                    ClientHandler.handleSet(List.of(parts), null); // silently apply
                                } else {
                                    System.out.println("Unhandled replication command: " + command);
                                }
                            }

                            commandBuffer.clear(); // clean parsed data
                        } catch (Exception e) {
                            System.out.println("Waiting for complete replication command: " + e.getMessage());
                        }
                    }
                } catch (IOException ioException) {
                    System.out.println("Replication listener stopped: " + ioException.getMessage());
                }
            }).start();
        }
            }

        
    

    private static void sendErrorResponse(OutputStream out, String error) throws IOException {
        out.write(("-ERR " + error + "\r\n").getBytes());
    }
    
}