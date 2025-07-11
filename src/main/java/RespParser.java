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
        return Integer.parseInt(sb.substring(1));
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

}
