import java.io.IOException;

class RespParser {
    private byte[] data;
    private int pos;

    public RespParser(byte[] data) {
        this.data = data;
        this.pos = 0;
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
            case '*':
            	int length = parseLength();
                String[] elements = new String[length];
                for (int i = 0; i < length; i++) {
                    elements[i] = parseString();
                }
                return new RespCommand(elements);
            case '$':
            	String value = parseString();
                return new RespCommand(new String[] { value });
            case '+':
            	String simpleString = parseSimpleStringValue();
                return new RespCommand(new String[] { simpleString });
            case '-':  // Error message
                String errorMessage = parseSimpleStringValue();
                throw new IOException("RESP Error: " + errorMessage);
            case ':':  // Integer type (not typically used in replication but can be useful)
                String integerValue = parseSimpleStringValue();
                return new RespCommand(new String[] { integerValue });
            default:
                throw new IOException("Unsupported RESP type: " + (char) type);
        }
    }

    private String parseString() throws IOException {
        int length = parseLength();
        if (length == -1) {
            return null;
        }
        String value = new String(data, pos, length);
        pos += length + 2;
        return value;
    }

    private int parseLength() throws IOException {
        StringBuilder sb = new StringBuilder();
        while (pos < data.length) {
            byte b = data[pos];
            pos++;
            if (b == '\r') {
                pos++; // Skip \n
                break;
            }
            sb.append((char) b);
        }
        return Integer.parseInt(sb.toString().substring(1));
    }
    
    private String parseSimpleStringValue() throws IOException {
        StringBuilder sb = new StringBuilder();
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
