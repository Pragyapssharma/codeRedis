import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
    public static void main(String[] args) {
        // You can use print statements as follows for debugging, they'll be visible when running tests.
        System.out.println("Logs from your program will appear here!");

        // Initialize variables
        ServerSocket serverSocket = null;
        Socket clientSocket = null;
        int port = 6379;

        try {
            // Set up the server socket and wait for client connections
            serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);
            System.out.println("Server is listening on port " + port);

            // Wait for a client connection
            clientSocket = serverSocket.accept();
            System.out.println("Client connected.");

            // Create an input stream to read commands from the client
            BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            OutputStream out = clientSocket.getOutputStream();

            String inputLine;
            // Process multiple PING commands from the same connection
            while ((inputLine = in.readLine()) != null) {
                if (inputLine.equals("PING")) {
                    // Send the PONG response for each PING command
                    out.write("+PONG\r\n".getBytes());
                } else {
                    // If we get something other than PING, we could log it or just ignore it
                    System.out.println("Received unknown command: " + inputLine);
                }
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        } finally {
            try {
                if (clientSocket != null) {
                    clientSocket.close();
                }
                if (serverSocket != null) {
                    serverSocket.close();
                }
            } catch (IOException e) {
                System.out.println("IOException during cleanup: " + e.getMessage());
            }
        }
    }
}
