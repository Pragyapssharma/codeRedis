import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;

class ClientHandler extends Thread {
    private Socket clientSocket;

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

                // Determine if the input is a PING or ECHO command
                if (inputLine.startsWith("*")) {
                    // Process the command
                    String command = readCommand(in);
                    if (command.equalsIgnoreCase("PING")) {
                        // RESP format for simple string PONG: +PONG\r\n
                        out.write("+PONG\r\n".getBytes());
                    } else if (command.equalsIgnoreCase("ECHO")) {
                        String argument = readArgument(in);
                        // RESP format for bulk string: $<length>\r\n<argument>\r\n
                        String response = "$" + argument.length() + "\r\n" + argument + "\r\n";
                        out.write(response.getBytes());
                    } else {
                        // If we get an unknown command, just log it for now
                        System.out.println("Unknown command: " + command);
                    }
                } else {
                    // If the command doesn't match expected format, print an error
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
        in.readLine(); // Read $4 (length of the command)
        return in.readLine().trim(); // Read the actual command (e.g., PING or ECHO)
    }

    // Helper function to read the argument (e.g., "hey" for ECHO)
    private String readArgument(BufferedReader in) throws IOException {
        in.readLine(); // Read $3 (length of the argument)
        return in.readLine().trim(); // Read the actual argument (e.g., "hey")
    }
}