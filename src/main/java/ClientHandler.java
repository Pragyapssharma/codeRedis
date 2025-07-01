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
        // Read the key and value for the SET command
        String key = readArgument(in);  // The key
        String value = readArgument(in);  // The value

        // Default expiration is 0 (no expiration)
        long expiryTimeMillis = 0;

        // Read the next argument to check if it's "px"
        String nextArg = readArgument(in); // Might be "px"
        
        if (nextArg != null && nextArg.equalsIgnoreCase("px")) {
            // If it is "px", read the expiration time in milliseconds
            expiryTimeMillis = Long.parseLong(readArgument(in));  // Expiry in milliseconds
        } else {
            // If no "px" argument, just return the key-value without expiration
            // We can ignore this extra argument for now (it will be null in case of no "px")
        }

        // Calculate the expiration timestamp (if expiration time is specified)
        long expirationTimestamp = expiryTimeMillis > 0 ? System.currentTimeMillis() + expiryTimeMillis : 0;

        // Store the key-value pair with expiration (if any)
        keyValueStore.put(key, new KeyValue(value, expirationTimestamp));

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
