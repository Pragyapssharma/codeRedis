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
                // Example input: *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
                if (inputLine.startsWith("*2")) {
                    String command = readCommand(in);
                    String argument = readArgument(in);

                    // Handle ECHO command
                    if (command.equalsIgnoreCase("ECHO")) {
                        // RESP format for bulk string: $<length>\r\n<argument>\r\n
                        String response = "$" + argument.length() + "\r\n" + argument + "\r\n";
                        out.write(response.getBytes());
                    } else {
                        // If the command is not recognized, print a log (or handle errors)
                        System.out.println("Received unknown command: " + command);
                    }
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

    // Helper function to read the command (e.g., "ECHO")
    private String readCommand(BufferedReader in) throws IOException {
        // The command is in the second line of the input (after *2\r\n$4\r\n)
        in.readLine(); // Read $4 (length of ECHO)
        return in.readLine().trim(); // Read the actual command (ECHO)
    }

    // Helper function to read the argument (e.g., "hey")
    private String readArgument(BufferedReader in) throws IOException {
        // The argument is in the fourth line (after $3\r\nhey\r\n)
        in.readLine(); // Read $3 (length of "hey")
        return in.readLine().trim(); // Read the actual argument (hey)
    }
}