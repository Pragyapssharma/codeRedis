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
        System.out.println("DEBUG: Received type: " + (char) type);
        pos++;
        
        System.out.println("DEBUG: Raw Data at position " + pos + ": " + Arrays.toString(data));

        switch (type) {
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
            case '$': // Bulk string (e.g., GET foo)
            	return parseBulkStringResponse();
//            	String value = parseString();
//            	System.out.println("DEBUG: Bulk String value: " + value);
//                return new RespCommand(new String[] { value });
            case '+': // Simple string (e.g., response like PONG from a PING command)
            	return parseSimpleStringResponse();
//            	String simpleString = parseSimpleStringValue();
//            	System.out.println("DEBUG: Simple String: " + simpleString);
//                return new RespCommand(new String[] { simpleString });
            case '-':  // Error message
//            	return parseErrorResponse();
                String errorMessage = parseSimpleStringValue();
                System.out.println("DEBUG: Error message: " + errorMessage);
                throw new IOException("RESP Error: " + errorMessage);
            case ':':  // Integer type (not typically used in replication but can be useful)
//            	return parseIntegerResponse();
                String integerValue = parseSimpleStringValue();
                System.out.println("DEBUG: Integer value: " + integerValue);
                return new RespCommand(new String[] { integerValue });
            default:
                throw new IOException("Unsupported RESP type: " + (char) type);
        }
    }
    
    
    private RespCommand parseArrayResponse() throws IOException {
    	System.out.println("DEBUG: Entering array response parsing.");
        int length = parseLength();
        System.out.println("DEBUG: Parsing array of length: " + length);
        RespCommand[] elements = new RespCommand[length];
        for (int i = 0; i < length; i++) {
            elements[i] = next();  // Recursively parse each element in the array
            if (elements[i] == null) throw new IOException("Null element in array");
        }
        return new RespCommand(elements);
    }

    private RespCommand parseBulkStringResponse() throws IOException {
        String value = parseString();  // Parse the bulk string value
        System.out.println("DEBUG: Bulk String value: " + value);
        return new RespCommand(new String[] { value });  // Wrap it in a single-element RespCommand
    }

    private RespCommand parseSimpleStringResponse() throws IOException {
        String simpleString = parseSimpleStringValue();
        System.out.println("DEBUG: Simple String: " + simpleString);
        return new RespCommand(new String[] { simpleString });
    }

    private RespCommand parseErrorResponse() throws IOException {
        String errorMessage = parseSimpleStringValue();
        throw new IOException("RESP Error: " + errorMessage);
    }

    private RespCommand parseIntegerResponse() throws IOException {
        String integerValue = parseSimpleStringValue();
        return new RespCommand(new String[] { integerValue });
    }
    

    private String parseString() throws IOException {
        int length = parseLength();
        System.out.println("DEBUG: Bulk String Length: " + length);
        if (length == -1) {
            return null;
        }
        
        if (pos + length + 2 >= data.length) {
            throw new IOException("Invalid or incomplete bulk string");
        }

//        if (pos >= data.length || pos + length + 2 > data.length) {
//            throw new IOException("Invalid or incomplete bulk string length: " + length);
//        }
        
        String value = new String(data, pos, length);
        pos += length;
        
//        if (pos + 1 >= data.length || data[pos] != '\r' || data[pos + 1] != '\n') {
//            throw new IOException("Bulk string not terminated correctly");
//        }
        
        if (data[pos] != '\r' || data[pos + 1] != '\n') {
            throw new IOException("Bulk string not terminated correctly");
        }

        pos += 2;
        System.out.println("DEBUG: Bulk String value parsed: " + value);
        return value;
    }
    
    
    private int parseLength() throws IOException {
        String num = parseLine();
        try {
            return Integer.parseInt(num);
        } catch (NumberFormatException e) {
            throw new IOException("Invalid length format: " + num, e);
        }

    }
    
    private String parseLine() throws IOException {
        if (pos >= data.length) throw new IOException("Incomplete line");
        int start = pos;
        while (pos < data.length) {
            if (data[pos] == '\r' && pos + 1 < data.length && data[pos+1] == '\n') {
                String line = new String(data, start, pos - start);
                pos += 2;
                return line;
            }
            pos++;
        }
        throw new IOException("Line not terminated");
    }

//    private int parseLength() throws IOException {
//        StringBuilder sb = new StringBuilder();
//        while (pos < data.length) {
//            byte b = data[pos];
//            pos++;
//            if (b == '\r') {
//                pos++; // Skip \n
//                break;
//            }
//            sb.append((char) b);
//        }
//        return Integer.parseInt(sb.toString().substring(1));
//    }
    
    private String parseSimpleStringValue() throws IOException {
        StringBuilder sb = new StringBuilder();
        System.out.println("DEBUG: Simple String value parsed: " + sb);
        while (pos < data.length) {
            byte b = data[pos];
            pos++;
            if (b == '\r') {
                pos++; // Skip \n
                break;
            }
            sb.append((char) b);
        }
        return sb.toString();
    }

    public void handleReplicationCommand(RespCommand command) throws IOException {
        String[] elements = command.getArray();

        if (elements[0].equals("FULLRESYNC")) {
            String replicationId = elements[1];
            long offset = Long.parseLong(elements[2]);

            // Handle the FULLRESYNC logic (you can use replicationId and offset here)
            System.out.println("Full Resync received from master: " + replicationId + " offset: " + offset);
            // Example logic, like initializing a new RDB file or syncing
            // You can store the replication state here or handle command propagation
        } else {
            System.out.println("Unsupported replication command: " + elements[0]);
        }
    }

    
}
