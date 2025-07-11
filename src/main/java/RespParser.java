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
                return parseArray();
            default:
                throw new IOException("Unsupported RESP type: " + (char) type);
        }
    }

    private RespCommand parseArray() throws IOException {
        int length = parseLength();
        String[] elements = new String[length];

        for (int i = 0; i < length; i++) {
            elements[i] = parseBulkString();
        }

        return new RespCommand(elements);
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

    private String parseBulkString() throws IOException {
        int length = parseLength();
        String value = new String(data, pos, length);
        pos += length + 2; // Skip \r\n
        return value;
    }
}

// RespCommand class
class RespCommand {
    private String[] array;

    public RespCommand(String[] array) {
        this.array = array;
    }

    public String[] getArray() {
        return array;
    }
}