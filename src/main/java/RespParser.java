import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;

class RespParser {
    private byte[] data;
    private int pos;

    public RespParser(byte[] data) {
        this.data = data;
        this.pos = 0;
    }
    
    public int getPos() {
        return pos;
    }

    public boolean hasNext() {
        return pos < data.length;
    }

    public RespCommand next() throws IOException {
        if (!hasNext()) {
            return null;
        }
        

        byte type = data[pos];
        pos++;
        

        switch (type) {
        	case '+': // Simple string (e.g., response like PONG from a PING command)
        		return parseSimpleStringResponse();
//        	String simpleString = parseSimpleStringValue();
//        	System.out.println("DEBUG: Simple String: " + simpleString);
//            return new RespCommand(new String[] { simpleString });
        	
        	case '$': // Bulk string (e.g., GET foo)
            	return parseBulkStringResponse();
//            	String value = parseString();
//            	System.out.println("DEBUG: Bulk String value: " + value);
//                return new RespCommand(new String[] { value });
        	
        	
            case '*': // Multi-element responses (e.g., MGET, LRANGE)
            	return parseArrayResponse();
//            	int length = parseLength();
//            	System.out.println("DEBUG: Parsing array of length: " + length);
//            	RespCommand[] elements = new RespCommand[length];
//                for (int i = 0; i < length; i++) {
//                    elements[i] = next();
//                    if (elements[i] == null) throw new IOException("Null element in array");
//                }
//                return new RespCommand(elements);
            
            
            case '-':  // Error message
//            	return parseErrorResponse();
                String errorMessage = parseSimpleStringValue();
                throw new IOException("RESP Error: " + errorMessage);
                
            case ':':  // Integer type (not typically used in replication but can be useful)
//            	return parseIntegerResponse();
                String integerValue = parseSimpleStringValue();
                return new RespCommand(integerValue);
                
            default:
                throw new IOException("Unsupported RESP type: " + (char) type);
        }
    }
    
    private RespCommand parseSimpleStringResponse() throws IOException {
        String value = parseSimpleStringValue();
        return new RespCommand(value);
    }

    private RespCommand parseBulkStringResponse() throws IOException {
        int length = parseLength();
        if (length == -1) {
            return new RespCommand((String) null);
        }
        if (pos + length + 2 > data.length) {
            throw new IOException("Invalid or incomplete bulk string");
        }
        String value = new String(data, pos, length);
        pos += length;
        if (data[pos] != '\r' || data[pos + 1] != '\n') {
            throw new IOException("Bulk string not terminated correctly");
        }
        pos += 2;
        return new RespCommand(value);
    }

    private RespCommand parseArrayResponse() throws IOException {
        int length = parseLength();
        if (length == -1) {
            return new RespCommand(new RespCommand[0]); // Null array treated as empty array
        }
        RespCommand[] elements = new RespCommand[length];
        for (int i = 0; i < length; i++) {
            elements[i] = next();
            if (elements[i] == null) {
                throw new IOException("Null element in array");
            }
        }
        return new RespCommand(elements);
    }
    
    
//    private RespCommand parseSimpleStringResponse() throws IOException {
//        String simpleString = parseSimpleStringValue();
//        return new RespCommand(new String[] { simpleString });
//    }

    private RespCommand parseIntegerResponse() throws IOException {
        String integerValue = parseSimpleStringValue();
        return new RespCommand(integerValue);
    }

//    private RespCommand parseBulkStringResponse() throws IOException {
//        int length = parseLength();
//        if (length == -1) {
//            return new RespCommand(new String[] { null });
//        }
//
//        if (pos + length + 2 > data.length) {
//            throw new IOException("Invalid or incomplete bulk string");
//        }
//
//        String value = new String(data, pos, length);
//        pos += length;
//
//        if (data[pos] != '\r' || data[pos + 1] != '\n') {
//            throw new IOException("Bulk string not terminated correctly");
//        }
//        pos += 2; // Skip \r\n
//
//        return new RespCommand(new String[] { value });
//    }

//    private RespCommand parseArrayResponse() throws IOException {
//        int length = parseLength();
//        if (length == -1) {
//            // Null array, treat as empty array or null?
//            return new RespCommand(new RespCommand[0]);
//        }
//
//        RespCommand[] elements = new RespCommand[length];
//        for (int i = 0; i < length; i++) {
//            elements[i] = next();
//            if (elements[i] == null) {
//                throw new IOException("Null element in array");
//            }
//        }
//        return new RespCommand(elements);
//    }

    private String parseSimpleStringValue() throws IOException {
        StringBuilder sb = new StringBuilder();
        while (pos < data.length) {
            byte b = data[pos++];
            if (b == '\r') {
                if (pos < data.length && data[pos] == '\n') {
                    pos++;
                    break;
                } else {
                    throw new IOException("Expected \\n after \\r in simple string");
                }
            }
            sb.append((char) b);
        }
        return sb.toString();
    }

    private int parseLength() throws IOException {
        String numStr = parseLine();
        try {
            return Integer.parseInt(numStr);
        } catch (NumberFormatException e) {
            throw new IOException("Invalid length: " + numStr);
        }
    }

    private String parseLine() throws IOException {
        if (pos >= data.length) throw new IOException("Incomplete line");

        int start = pos;
        while (pos < data.length) {
            if (data[pos] == '\r' && pos + 1 < data.length && data[pos + 1] == '\n') {
                String line = new String(data, start, pos - start);
                pos += 2; // Skip \r\n
                return line;
            }
            pos++;
        }
        throw new IOException("Line not terminated");
    }
    
    public void handleReplicationCommand(RespCommand command) throws IOException {
        if (command.isSimple()) {
            String val = command.getValue();  // e.g. "+FULLRESYNC <replid> <offset>"
            if (val.startsWith("FULLRESYNC")) {
                String[] parts = val.split(" ");
                if (parts.length >= 3) {
                    String replicationId = parts[1];
                    long offset = Long.parseLong(parts[2]);
                    System.out.println("Received FULLRESYNC command");
                    System.out.println("ReplicationId: " + replicationId + ", offset: " + offset);
                    // handle FULLRESYNC logic here
                } else {
                    throw new IOException("Invalid FULLRESYNC simple string format");
                }
            } else {
                System.out.println("Received simple command: " + val);
            }
        } else if (command.getArray() != null) {
            // Handle array commands here if needed
            System.out.println("Received array command with subcommands");
        } else {
            throw new IOException("Unexpected command format");
        }
    }



    // Example usage of handleReplicationCommand based on your example:
//    public void handleReplicationCommand(RespCommand command) throws IOException {
//        if (command.getSubCommands() != null) {
//            // Array of RespCommands
//            RespCommand[] elements = command.getSubCommands();
//            if (elements.length > 0) {
//                String cmdName = elements[0].getArray() != null ? elements[0].getArray()[0] : null;
//                if ("FULLRESYNC".equalsIgnoreCase(cmdName)) {
//                    // Handle FULLRESYNC command here
//                    System.out.println("Received FULLRESYNC command");
//                    // Example: get replicationId and offset
//                    if (elements.length >= 3) {
//                        String replicationId = elements[1].getArray()[0];
//                        String offsetStr = elements[2].getArray()[0];
//                        long offset = Long.parseLong(offsetStr);
//                        System.out.println("ReplicationId: " + replicationId + ", offset: " + offset);
//                    }
//                } else {
//                    System.out.println("Unhandled replication command: " + cmdName);
//                }
//            }
//        } else if (command.getArray() != null) {
//            String[] parts = command.getArray();
//            if (parts.length > 0) {
//                if ("FULLRESYNC".equalsIgnoreCase(parts[0])) {
//                    System.out.println("Received FULLRESYNC (flat array) with parts: " + Arrays.toString(parts));
//                    // Handle FULLRESYNC logic here
//                } else {
//                    System.out.println("Unhandled replication command: " + parts[0]);
//                }
//            }
//        } else {
//            throw new IOException("Unexpected command format");
//        }
//    }
    
}
