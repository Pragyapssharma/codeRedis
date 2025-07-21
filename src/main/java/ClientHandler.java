import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

class ClientHandler extends Thread {
    private Socket clientSocket;
    private InputStream in;
    private OutputStream out;
    private static final Map<String, KeyValue> keyValueStore = new ConcurrentHashMap<>();
    private static final List<OutputStream> replicaOutputs = new CopyOnWriteArrayList<>();
    private static final byte[] EMPTY_RDB_FILE = new byte[] {
    	    (byte) 0x52, (byte) 0x45, (byte) 0x44, (byte) 0x49, // REDI
    	    (byte) 0x53, (byte) 0x30, (byte) 0x30, (byte) 0x30, (byte) 0x39, // S0009
    	    (byte) 0xFA, (byte) 0x00, // auxiliary field with 0 length
    	    (byte) 0xFF,             // end-of-RDB opcode
    	    (byte) 0x00, (byte) 0x00, // dummy checksum
    	    (byte) 0x00, (byte) 0x00  // padding (some implementations expect 18 bytes total)
    	};

//    private boolean isReplicaConnection = false;

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
            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                if (!inputLine.startsWith("*")) {
                    out.write("-ERR Invalid command format\r\n".getBytes("UTF-8"));
                    continue;
                }

                int argCount = Integer.parseInt(inputLine.substring(1));
                List<String> args = readArguments(in, argCount);
                if (args.isEmpty()) continue;

                String command = args.get(0).toUpperCase();

                switch (command) {
                    case "PING":
                        out.write("+PONG\r\n".getBytes("UTF-8"));
                        break;
                        
                    case "REPLCONF":
                        out.write("+OK\r\n".getBytes("UTF-8"));
                        break;


                    case "ECHO":
                        if (args.size() >= 2) {
                            String echo = args.get(1);
                            out.write(("$" + echo.length() + "\r\n" + echo + "\r\n").getBytes("UTF-8"));
                        } else {
                            out.write("-ERR wrong number of arguments for 'ECHO'\r\n".getBytes("UTF-8"));
                        }
                        break;

                    case "SET":
                        handleSet(args, out, false);
                        break;

                    case "GET":
                        handleGet(args, out);
                        break;

                    default:
                        out.write("-ERR unknown command\r\n".getBytes("UTF-8"));
                        break;
                }
            }
        } catch (IOException e) {
            System.err.println("Client connection error: " + e.getMessage());
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                System.err.println("Error closing client socket: " + e.getMessage());
            }
        }
    }

    
    
    
//    public void run() {
//        try (
//            BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
//            OutputStream out = clientSocket.getOutputStream()
//        ) {
//        	this.out = out;
//        	if (Config.isReplica && !isReplicaConnection) {
//        	    processMasterHandshake(in, out);
//        	    isReplicaConnection = true;
//        	}
//            String inputLine;
//            while ((inputLine = in.readLine()) != null) {
//                System.out.println("Received: " + inputLine);
//
//                if (inputLine.startsWith("*")) {
//                    int argCount = Integer.parseInt(inputLine.substring(1));
//                    List<String> args = readArguments(in, argCount);
//                    System.out.println("📦 Parsed RESP args: " + args);
//
//                    if (args.isEmpty()) continue;
//
//                    String command = args.get(0).toUpperCase();
//                    
////                    if (Config.isReplica) {
////                        // Process replicated command silently
////                        switch (command) {
////                            case "SET":
////                                handleSet(args, null); // null OutputStream => no reply
////                                break;
////                            default:
////                                System.out.println("Replica received unsupported command: " + command);
////                                break;
////                        }
////                        continue;
////                    }
////                    if (isReplicaConnection && (command.equals("SET") || command.equals("PING") || command.equals("ECHO"))) {
//                        // Only swallow known replication commands; let other commands go through
//                    if (Config.isReplica && isReplicaConnection) {
//
//                        switch (command) {
//                            case "SET":
//                                handleSet(args, null, true);
//                                break;
//                            case "PING":
//                                out.write("+PONG\r\n".getBytes());
//                                break;
//                            case "ECHO":
//                                if (args.size() >= 2) {
//                                    String echo = args.get(1);
//                                    out.write(("$" + echo.length() + "\r\n" + echo + "\r\n").getBytes());
//                                }
//                                break;
//                        }
//                        continue;
//                    }
//
//                    switch (command) {
//                        case "PING":
//                            out.write("+PONG\r\n".getBytes());
//                            break;
//                            
//                        case "REPLCONF":
//                            out.write("+OK\r\n".getBytes());
//                            break;
//                            
//                        case "PSYNC":
////                        	if (args.size() == 3 && args.get(1).equals("?") && args.get(2).equals("-1")) {
////                                String replId = Config.masterReplId;
////                                String fullResync = "+FULLRESYNC " + replId + " 0\r\n";
////                                out.write(fullResync.getBytes());
////
////                                // Send empty RDB
////                                byte[] rdbBytes = EMPTY_RDB_FILE;
////                                String header = "$" + rdbBytes.length + "\r\n";
////                                out.write(header.getBytes());
////                                out.write(rdbBytes);
////                                System.out.println("Sent FULLRESYNC and empty RDB file (" + rdbBytes.length + " bytes)");
////
////                                // Mark this as replica
////                                isReplicaConnection = true;
////                                replicaOutputs.add(out);
////                                System.out.println("Added new replica connection. Total: " + replicaOutputs.size());
////                            } else {
////                                out.write("-ERR unsupported PSYNC format\r\n".getBytes());
////                            }
//                        	handlePsync(args, out);
//                            break;
//
//
//                        case "ECHO":
//                            if (args.size() >= 2) {
//                                String echo = args.get(1);
//                                String response = "$" + echo.length() + "\r\n" + echo + "\r\n";
//                                out.write(response.getBytes());
//                            }
//                            break;
//
//                        case "SET":
//                            handleSet(args, out, false);
//                            break;
//
//                        case "GET":
//                            handleGet(args, out);
//                            break;
//                        
//                        case "CONFIG":
//                            handleConfig(args, out);
//                            break;
//                            
//                        case "KEYS":
//                            handleKeys(args, out);
//                            break;
//                            
//                        case "INFO":
//                            handleInfo(args, out);
//                            break;
//
//                        default:
//                            System.out.println("Unknown command: " + command);
//                            out.write(("-ERR unknown command\r\n").getBytes());
//                            break;
//                    }
//                } else {
//                    System.out.println("Invalid command format: " + inputLine);
//                }
//            }
//        } catch (IOException e) {
//            System.out.println("IOException handling client: " + e.getMessage());
//        } finally {
//            try {
//                if (clientSocket != null) {
//                    clientSocket.close();
//                }
//            } catch (IOException e) {
//                System.out.println("IOException during client socket cleanup: " + e.getMessage());
//            } finally {
//            	if (isReplicaConnection) {
//                    replicaOutputs.remove(out);
//                    System.out.println("Replica removed. Remaining: " + replicaOutputs.size());
//                }
//            }
//        }
//    }

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

    public static void handleSet(List<String> args, OutputStream out, boolean suppressPropagation) throws IOException {
        if (args.size() < 3) {
        	if (out != null) {
            out.write(("-ERR wrong number of arguments for 'SET'\r\n").getBytes("UTF-8"));
            return;
        	}
        }

        String key = args.get(1);
        String value = args.get(2);
        
        long expiryMillis = 0;
        if (args.size() >= 5 && args.get(3).equalsIgnoreCase("px")) {
            try {
                expiryMillis = Long.parseLong(args.get(4));
            } catch (NumberFormatException e) {
            	if (out != null) {
                    out.write(("-ERR PX value is not a number\r\n").getBytes("UTF-8"));
                }
                return;
            }
        }

        long expirationTimestamp = expiryMillis > 0 ? System.currentTimeMillis() + expiryMillis : 0;
        keyValueStore.put(key, new KeyValue(value, expirationTimestamp));
        
        System.out.println("All stored keys:");
        for (String key1 : keyValueStore.keySet()) {
            System.out.println("  " + key1);
        }
        
        System.out.println("SET applied: " + key + " -> " + value);
        
        System.out.println("All stored keys:");
        for (String k : keyValueStore.keySet()) {
            System.out.println("  " + k);
        }

        if (out != null) {
            out.write("+OK\r\n".getBytes("UTF-8"));
        }
        
        if (!suppressPropagation) {
            ReplicationHandler.propagateSetToReplicas(key, value);
        }
    }
    
    private void handleGet(List<String> args, OutputStream out) throws IOException {
        if (args.size() != 2) {
            out.write("-ERR wrong number of arguments for 'GET'\r\n".getBytes("UTF-8"));
            return;
        }

        String key = args.get(1);
        KeyValue kv = keyValueStore.get(key);
        
        if (kv == null || kv.hasExpired()) {
            out.write("$-1\r\n".getBytes("UTF-8")); // Null bulk string
        } else {
            String value = kv.value;
            byte[] valueBytes = value.getBytes("UTF-8");
            System.out.println("GET request received from client. Responding with: $" + valueBytes.length + " " + kv.value);
            out.write(("$" + valueBytes.length + "\r\n").getBytes("UTF-8"));
            out.write(valueBytes);
            out.write("\r\n".getBytes("UTF-8"));
        }
    }
    
    
    public static void handleConfig(List<String> args, OutputStream out) throws IOException {
        if (args.size() < 3) {
            out.write(("-ERR wrong number of arguments for 'CONFIG GET'\r\n").getBytes());
            return;
        }

        String configKey = args.get(2);
        String value;

        switch (configKey) {
            case "dir":
                value = Config.dir;
                break;
            case "dbfilename":
                value = Config.dbFilename;
                break;
            case "port":
                value = String.valueOf(Config.port);
                break;
            case "replica":
                value = String.valueOf(Config.isReplica);
                break;
            default:
                out.write(("-ERR unknown configuration parameter\r\n").getBytes());
                return;
        }

        // Return array of 2 bulk strings: key and value
        String response = "*" + 2 + "\r\n" +
                          "$" + configKey.length() + "\r\n" + configKey + "\r\n" +
                          "$" + value.length() + "\r\n" + value + "\r\n";
        out.write(response.getBytes());
    }

    
    public static void handleKeys(List<String> args, OutputStream out) throws IOException {
        StringBuilder keysResponse = new StringBuilder("*" + keyValueStore.size() + "\r\n");
        for (String key : keyValueStore.keySet()) {
            keysResponse.append("$").append(key.length()).append("\r\n").append(key).append("\r\n");
        }
        out.write(keysResponse.toString().getBytes());
    }
    
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
        if (!Config.isReplica) {
            // This server is the master and is handling a replica's PSYNC request
            if (args.size() < 3) {
                out.write(("-ERR wrong number of arguments for 'PSYNC'\r\n").getBytes());
                return;
            }

            String replicationId = args.get(1);  // e.g., "?"
            long offset = Long.parseLong(args.get(2));  // e.g., -1

            System.out.println("Replica requested PSYNC: replid=" + replicationId + ", offset=" + offset);

            // Respond with FULLRESYNC
            String syncResponse = "+FULLRESYNC " + Config.masterReplId + " 0\r\n";
            out.write(syncResponse.getBytes());

            // Send empty RDB file as bulk string
            byte[] rdbBytes = EMPTY_RDB_FILE;
            String header = "$" + rdbBytes.length + "\r\n";
            out.write(header.getBytes());
            out.write(rdbBytes);

            // Track this replica output stream for future propagation
            replicaOutputs.add(out);
            System.out.println("Replica added: total=" + replicaOutputs.size());

        } else {
            out.write(("-ERR PSYNC is only valid for master servers\r\n").getBytes());
        }
    }

    private static void sendErrorResponse(OutputStream out, String error) throws IOException {
        out.write(("-ERR " + error + "\r\n").getBytes());
    }
    
}