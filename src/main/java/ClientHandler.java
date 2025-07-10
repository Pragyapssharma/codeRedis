import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class ClientHandler extends Thread {
    private Socket clientSocket;
    private static final Map<String, KeyValue> keyValueStore = new HashMap<>();

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
                System.out.println("Received: " + inputLine);

                if (inputLine.startsWith("*")) {
                    int argCount = Integer.parseInt(inputLine.substring(1));
                    List<String> args = readArguments(in, argCount);

                    if (args.isEmpty()) continue;

                    String command = args.get(0).toUpperCase();
                    switch (command) {
                        case "PING":
                            out.write("+PONG\r\n".getBytes());
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
                out.write(("-ERR PX value is not a number\r\n").getBytes());
                return;
            }
        }

        long expirationTimestamp = expiryMillis > 0 ? System.currentTimeMillis() + expiryMillis : 0;
        keyValueStore.put(key, new KeyValue(value, expirationTimestamp));

        out.write("+OK\r\n".getBytes());
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

            // Always include role
            sb.append("role:").append(Config.isReplica ? "slave" : "master").append("\r\n");

            // Only master includes replication ID and offset
            if (!Config.isReplica) {
                sb.append("master_replid:").append(Config.masterReplId).append("\r\n");
                sb.append("master_repl_offset:").append(Config.masterReplOffset).append("\r\n");
            }

            // Ensure the final line ends with \r\n
            String info = sb.toString();
            if (!info.endsWith("\r\n")) {
                info += "\r\n";
            }

            byte[] infoBytes = info.getBytes("UTF-8");
            String response = "$" + infoBytes.length + "\r\n" + info;
            out.write(response.getBytes("UTF-8"));
        } else {
            out.write("-ERR unsupported INFO section\r\n".getBytes());
        }
    }



    
}