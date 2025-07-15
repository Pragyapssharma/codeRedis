import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

class ReplicationHandler {
    private static List<OutputStream> replicaOutputs = new ArrayList<>();
    private static final int MAX_RETRIES = 3;

    // Add a new replica connection
    public static void addReplica(OutputStream out) {
        replicaOutputs.add(out);
    }

    // Remove a replica connection
    public static void removeReplica(OutputStream out) {
        replicaOutputs.remove(out);
    }

    // Propagate the SET command to all replicas
    public static void propagateSetToReplicas(String key, String value) {
        StringBuilder command = new StringBuilder();
        command.append("*3\r\n");  // Length of the array (3 parts for "SET", key, and value)
        command.append("$3\r\nSET\r\n");
        command.append("$").append(key.length()).append("\r\n").append(key).append("\r\n");
        command.append("$").append(value.length()).append("\r\n").append(value).append("\r\n");

        byte[] commandBytes = command.toString().getBytes();

        // Send the SET command to each replica
        
        for (OutputStream replicaOut : replicaOutputs) {
            int attempts = 0;
            boolean success = false;

            while (attempts < MAX_RETRIES && !success) {
                try {
                    replicaOut.write(commandBytes);  // Send the SET command to the replica
                    replicaOut.flush();
                    success = true;  // Command sent successfully
                } catch (IOException e) {
                    attempts++;
                    System.out.println("Failed to propagate to replica (attempt " + attempts + "): " + e.getMessage());
                    if (attempts == MAX_RETRIES) {
                        removeReplica(replicaOut);  // Remove failed replica after max retries
                        System.out.println("Replica removed after " + MAX_RETRIES + " failed attempts.");
                    }
                }
            }
        
        }
//        for (OutputStream replicaOut : replicaOutputs) {
//            try {
//                replicaOut.write(commandBytes);  // Send the SET command to the replica
//                replicaOut.flush();
//            } catch (IOException e) {
//                System.out.println("Failed to propagate to replica: " + e.getMessage());
//                removeReplica(replicaOut);  // Remove failed replica connection
//            }
//        }
    }
}