import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

class RespParser {
    private final byte[] data;
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
        
        System.out.println("Parsed RESP type: " + (char) type);

        switch (type) {
        	case '+': // Simple string (e.g., response like PONG from a PING command)
        		return parseSimpleStringResponse();
        	
        	case '$': // Bulk string (e.g., GET foo)
            	return parseBulkStringResponse();
        	
            case '*': // Multi-element responses (e.g., MGET, LRANGE)
            	return parseArrayResponse();
            
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
//        String value = new String(data, pos, length);
        String value = new String(data, pos, length, StandardCharsets.UTF_8);
        pos += length;
        System.out.println("debug - bulk :"+value);
        if (data[pos] != '\r' || data[pos + 1] != '\n') {
            throw new IOException("Bulk string not terminated correctly");
        }
        pos += 2;
        return new RespCommand(value);
    }
    
    private RespCommand parseArrayResponse() throws IOException {
        int length = parseLength();
        if (length == -1) {
            return new RespCommand(new String[0]);
        }

        List<String> values = new ArrayList<>();
        for (int i = 0; i < length; i++) {
            RespCommand element = next();
            if (element == null) throw new IOException("Null element in array");
            values.add(element.getValue());
        }

        return new RespCommand(values.toArray(new String[0]));
    }

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
                String line = new String(data, start, pos - start, StandardCharsets.UTF_8);
                pos += 2; // Skip \r\n
                return line;
            }
            pos++;
        }
        throw new IOException("Line not terminated");
    }
    
    public void handleReplicationCommand(RespCommand command) throws IOException {
        if (command.isSimple()) {
            String val = command.getValue();
            if (val.startsWith("FULLRESYNC")) {
                String[] parts = val.split(" ");
                if (parts.length >= 3) {
                    String replicationId = parts[1];
                    long offset = Long.parseLong(parts[2]);
                    System.out.println("Received FULLRESYNC command");
                    System.out.println("ReplicationId: " + replicationId + ", offset: " + offset);
                } else {
                    throw new IOException("Invalid FULLRESYNC simple string format");
                }
            } else {
                System.out.println("Received simple command: " + val);
            }
        } else {
            String[] parts = command.getArray(); // Uses the array of sub-RespCommands' values
            if (parts != null && parts.length > 0) {
                String cmdName = parts[0];
                if ("FULLRESYNC".equalsIgnoreCase(cmdName)) {
                    if (parts.length >= 3) {
                        String replicationId = parts[1];
                        long offset = Long.parseLong(parts[2]);
                        System.out.println("Received FULLRESYNC command");
                        System.out.println("ReplicationId: " + replicationId + ", offset: " + offset);
                    } else {
                        throw new IOException("FULLRESYNC command missing parameters");
                    }
                } else {
                    System.out.println("Unhandled replication command: " + cmdName);
                }
            } else {
                System.out.println("Received array command with no elements");
            }
        }
    }

}
