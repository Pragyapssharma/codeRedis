import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

class ReplicationHandler {
    private static List<OutputStream> replicaOutputs = new ArrayList<>();
    private static final int MAX_RETRIES = 3;

    public static void addReplica(OutputStream out) {
        replicaOutputs.add(out);
    }

    public static void removeReplica(OutputStream out) {
        replicaOutputs.remove(out);
    }

    public static void propagateSetToReplicas(String key, String value) {
        StringBuilder command = new StringBuilder();
        command.append("*3\r\n");
        command.append("$3\r\nSET\r\n");
        command.append("$").append(key.length()).append("\r\n").append(key).append("\r\n");
        command.append("$").append(value.length()).append("\r\n").append(value).append("\r\n");

        byte[] commandBytes = command.toString().getBytes();

        
        for (OutputStream replicaOut : replicaOutputs) {
            int attempts = 0;
            boolean success = false;

            while (attempts < MAX_RETRIES && !success) {
                try {
                    replicaOut.write(commandBytes);
                    replicaOut.flush();
                    success = true;
                } catch (IOException e) {
                    attempts++;
                    System.out.println("Failed to propagate to replica (attempt " + attempts + "): " + e.getMessage());
                    if (attempts == MAX_RETRIES) {
                        removeReplica(replicaOut); 
                        System.out.println("Replica removed after " + MAX_RETRIES + " failed attempts.");
                    }
                }
            }
        
        }

    }
}