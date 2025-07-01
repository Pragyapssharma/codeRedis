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
        if (args.size() < 2) {
            out.write(("-ERR wrong number of arguments for 'GET'\r\n").getBytes());
            return;
        }

        String key = args.get(1);
        KeyValue keyValue = keyValueStore.get(key);

        if (keyValue != null) {
            if (keyValue.hasExpired()) {
                keyValueStore.remove(key);
                out.write("$-1\r\n".getBytes());
            } else {
                String value = keyValue.value;
                out.write(("$" + value.length() + "\r\n" + value + "\r\n").getBytes());
            }
        } else {
            out.write("$-1\r\n".getBytes());
        }
    }

    static class KeyValue {
        String value;
        long expirationTimestamp;

        KeyValue(String value, long expirationTimestamp) {
            this.value = value;
            this.expirationTimestamp = expirationTimestamp;
        }

        boolean hasExpired() {
            return expirationTimestamp > 0 && System.currentTimeMillis() > expirationTimestamp;
        }
    }
}