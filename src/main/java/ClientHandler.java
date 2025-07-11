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

    private static OutputStream replicaOut = null;
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
                        	if (args.size() == 3 && args.get(1).equals("?") && args.get(2).equals("-1")) {
                                String replId = Config.masterReplId;
                                String fullResync = "+FULLRESYNC " + replId + " 0\r\n";
                                out.write(fullResync.getBytes());

                                // Send empty RDB
                                byte[] rdbBytes = EMPTY_RDB_FILE;
                                String header = "$" + rdbBytes.length + "\r\n";
                                out.write(header.getBytes());
                                out.write(rdbBytes);
                                System.out.println("Sent FULLRESYNC and empty RDB file (" + rdbBytes.length + " bytes)");

                                // Mark this as replica
                                isReplicaConnection = true;
                                replicaOutputs.add(out);
                                System.out.println("Added new replica connection. Total: " + replicaOutputs.size());
                            } else {
                                out.write("-ERR unsupported PSYNC format\r\n".getBytes());
                            }
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
                try {
                    if (clientSocket != null) clientSocket.close();
                } catch (IOException e) {
                    System.out.println("Socket close error: " + e.getMessage());
                }
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

    private void handleSet(List<String> args, OutputStream out) throws IOException {
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
                    replicaOut.write(commandBytes);
                    replicaOut.flush();
                } catch (IOException e) {
                    System.out.println("Replica write failed, removing: " + e.getMessage());
                    replicaOutputs.remove(replicaOut);
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

        if (kv == null || kv.hasExpired()) {
            System.out.println("GET " + key + " => not found or expired");
            out.write("$-1\r\n".getBytes());  // RESP nil bulk string
            return;
        }

        String value = kv.value;  // direct field access, or use kv.getValue() if you add getter
        out.write(("$" + value.length() + "\r\n" + value + "\r\n").getBytes());
    }

    static class KeyValue {
        String value;
        long expirationTimestamp;

        KeyValue(String value, long expirationTimestamp) {
            this.value = value;
            this.expirationTimestamp = expirationTimestamp;
        }
        
        boolean hasExpired() {
            long now = System.currentTimeMillis();
            boolean expired = expirationTimestamp > 0 && now > expirationTimestamp;
            System.out.printf("Checking if expired: now=%d, expire=%d, expired=%b%n", now, expirationTimestamp, expired);
            return expired;
        }
    }
    
    private void handleConfig(List<String> args, OutputStream out) throws IOException {
        if (args.size() >= 3 && args.get(1).equalsIgnoreCase("GET")) {
            String param = args.get(2).toLowerCase();
            String value;

            switch (param) {
                case "dir":
                    value = Config.dir;
                    break;
                case "dbfilename":
                    value = Config.dbFilename;
                    break;
                default:
                    value = "";
                    break;
            }

            // RESP array response with key and value
            String response = "*2\r\n" +
                              "$" + param.length() + "\r\n" + param + "\r\n" +
                              "$" + value.length() + "\r\n" + value + "\r\n";
            out.write(response.getBytes());
        } else {
            out.write("-ERR wrong number of arguments for CONFIG GET\r\n".getBytes());
        }
    }
    
    private void handleKeys(List<String> args, OutputStream out) throws IOException {

    	List<String> filteredKeys = new ArrayList<>();

        for (String key : keyValueStore.keySet()) {
            KeyValue kv = keyValueStore.get(key);
            if (!kv.hasExpired()) {
                filteredKeys.add(key);
            }
        }
        StringBuilder response = new StringBuilder("*" + filteredKeys.size() + "\r\n");
        for (String key : filteredKeys) {
            response.append("$").append(key.length()).append("\r\n").append(key).append("\r\n");
        }


        out.write(response.toString().getBytes());
    }
    
    private void handleInfo(List<String> args, OutputStream out) throws IOException {
        if (args.size() >= 2 && args.get(1).equalsIgnoreCase("replication")) {
            StringBuilder sb = new StringBuilder();

            sb.append("role:").append(Config.isReplica ? "slave" : "master").append("\r\n");
            if (!Config.isReplica) {
                sb.append("master_replid:").append(Config.masterReplId).append("\r\n");
                sb.append("master_repl_offset:").append(Config.masterReplOffset).append("\r\n");
            }

            String infoBody = sb.toString();
            byte[] infoBytes = infoBody.getBytes("UTF-8");
            String header = "$" + infoBytes.length + "\r\n";

            
            System.out.println("INFO output (" + infoBytes.length + " bytes):");
            for (String line : infoBody.split("\r\n")) {
                System.out.println("> " + line + " [\\r\\n]");
            }
            System.out.println("Header to send: " + header.replace("\r", "\\r").replace("\n", "\\n"));
            System.out.println("Full payload length: " + infoBytes.length);
            System.out.println("Sending full RESP:\n" + header + infoBody);

            out.write(header.getBytes("UTF-8"));
            out.write(infoBytes);
            out.write("\r\n".getBytes("UTF-8"));
        } else {
            out.write("-ERR unsupported INFO section\r\n".getBytes());
        }
    }

    private void processMasterHandshake(BufferedReader in, OutputStream out) throws IOException {
        // Send REPLCONF listening-port
        String replconfPort = "*3\r\n" +
                              "$8\r\nREPLCONF\r\n" +
                              "$14\r\nlistening-port\r\n" +
                              "$" + String.valueOf(Config.port).length() + "\r\n" +
                              Config.port + "\r\n";
        out.write(replconfPort.getBytes());

        // Send REPLCONF capa psync2
        String replconfCapa = "*3\r\n" +
                              "$8\r\nREPLCONF\r\n" +
                              "$4\r\ncapa\r\n" +
                              "$6\r\npsync2\r\n";
        out.write(replconfCapa.getBytes());

        // Send PSYNC ? -1
        String psync = "*3\r\n" +
                       "$5\r\nPSYNC\r\n" +
                       "$1\r\n?\r\n" +
                       "$2\r\n-1\r\n";
        out.write(psync.getBytes());

        out.flush();

        String line;
        // Wait for +FULLRESYNC
        while ((line = in.readLine()) != null) {
            if (line.startsWith("+FULLRESYNC")) {
                System.out.println("Received FULLRESYNC: " + line);
                break;
            }
        }

        // Expect RDB bulk string: $<length>
        line = in.readLine();
        if (line != null && line.startsWith("$")) {
            int rdbLength = Integer.parseInt(line.substring(1));
            char[] rdbBuffer = new char[rdbLength];
            int totalRead = 0;
            while (totalRead < rdbLength) {
                int read = in.read(rdbBuffer, totalRead, rdbLength - totalRead);
                if (read == -1) break;
                totalRead += read;
            }
            System.out.println("Read " + totalRead + " RDB bytes from master.");
        }
    }


    
}