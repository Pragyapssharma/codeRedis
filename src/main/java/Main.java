import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
    public static void main(String[] args) {
        System.out.println("Logs from your program will appear here!");

        // Initialize variables
        ServerSocket serverSocket = null;
        int port = 6379;

        try {
            // Set up the server socket and wait for client connections
            serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);
            System.out.println("Server is listening on port " + port);

            // Infinite loop to accept multiple clients concurrently
            while (true) {
                // Wait for a client connection
                Socket clientSocket = serverSocket.accept();
                System.out.println("Client connected.");

                // Spawn a new thread to handle the client
                new ClientHandler(clientSocket).start();
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        } finally {
            try {
                if (serverSocket != null) {
                    serverSocket.close();
                }
            } catch (IOException e) {
                System.out.println("IOException during cleanup: " + e.getMessage());
            }
        }
    }
}