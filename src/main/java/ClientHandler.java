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

    @Override
    public void run() {
        try (
            BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            OutputStream out = clientSocket.getOutputStream()
        ) {
            String inputLine;
            // Process multiple commands from the same connection
            while ((inputLine = in.readLine()) != null) {
                System.out.println("Received: " + inputLine);

                // Process the command based on the format
                if (inputLine.startsWith("*")) {
                    String command = readCommand(in);

                    switch (command.toUpperCase()) {
                        case "PING":
                            out.write("+PONG\r\n".getBytes());
                            break;

                        case "ECHO":
                            String argument = readArgument(in);
                            String echoResponse = "$" + argument.length() + "\r\n" + argument + "\r\n";
                            out.write(echoResponse.getBytes());
                            break;

                        case "SET":
                            handleSetCommand(in, out);
                            break;

                        case "GET":
                            handleGetCommand(in, out);
                            break;

                        default:
                            System.out.println("Unknown command: " + command);
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

    // Helper function to read the command (e.g., "PING" or "ECHO")
    private String readCommand(BufferedReader in) throws IOException {
        in.readLine(); // Read the $<length> (length of the command)
        return in.readLine().trim(); // Read the actual command (e.g., PING or ECHO)
    }

    // Helper function to read the argument (e.g., "hey" for ECHO)
    private String readArgument(BufferedReader in) throws IOException {
        in.readLine(); // Read $<length> (length of the argument)
        return in.readLine().trim(); // Read the actual argument (e.g., "hey")
    }

    private void handleSetCommand(BufferedReader in, OutputStream out) throws IOException {
        // Read the parameters of the SET command
        List<String> params = new ArrayList<>();
        params.add(readArgument(in));  // Key
        params.add(readArgument(in));  // Value

        // Check for "PX" expiration argument
        String nextArg = readArgument(in); // Might be "px"
        params.add(nextArg);  // Store the "px" if present

        if (nextArg != null && nextArg.equalsIgnoreCase("px")) {
            // If "px" exists, get the expiration time
            Long expiryTimeInMillis = Long.parseLong(readArgument(in));  // Expiration time in milliseconds
            long expirationTimestamp = System.currentTimeMillis() + expiryTimeInMillis;

            // Store the key-value pair with expiration
            keyValueStore.put(params.get(0), new KeyValue(params.get(1), expirationTimestamp));
        } else {
            // Store key-value pair without expiration
            keyValueStore.put(params.get(0), new KeyValue(params.get(1), 0));
        }

        // Respond with +OK for SET command
        out.write("+OK\r\n".getBytes());
    }



    private void handleGetCommand(BufferedReader in, OutputStream out) throws IOException {
        // Read the key for the GET command
        String key = readArgument(in);
        KeyValue keyValue = keyValueStore.get(key);

        if (keyValue != null) {
            // Check if the key has expired
            if (keyValue.hasExpired()) {
                // Remove expired key and return null bulk string
                keyValueStore.remove(key);
                out.write("$-1\r\n".getBytes());
            } else {
                // Return the value as a bulk string if the key has not expired
                out.write(("$" + keyValue.value.length() + "\r\n" + keyValue.value + "\r\n").getBytes());
            }
        } else {
            // Return null bulk string if the key does not exist
            out.write("$-1\r\n".getBytes());
        }
    }

    // Key-Value class with expiration support
    static class KeyValue {
        String value;
        long expirationTimestamp;

        KeyValue(String value, long expirationTimestamp) {
            this.value = value;
            this.expirationTimestamp = expirationTimestamp;
        }

        boolean hasExpired() {
            // If the expirationTimestamp is greater than 0 and the current time is after the expiration, the key has expired
            return expirationTimestamp > 0 && System.currentTimeMillis() > expirationTimestamp;
        }
    }
}
