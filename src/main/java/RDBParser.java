import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

public class RDBParser {
    public static void loadFromStream(InputStream stream) throws IOException {
        DataInputStream in = new DataInputStream(stream);

        // Read header: "REDIS" + version (magic 5 bytes + 4 version bytes)
        byte[] header = new byte[9];
        in.readFully(header);

        while (true) {
            int opcode = in.readUnsignedByte();

            if (opcode == 0xFE) { // SELECT DB opcode
                in.readByte(); // skip DB ID
            } else if (opcode == 0xFD) { // EXPIRETIME_MS (ignore)
                in.readLong(); // skip
            } else if (opcode == 0x00) { // String type
                String key = readLengthEncodedString(in);
                String value = readLengthEncodedString(in);
//                ClientHandler.keyValueStore.put(key, new ClientHandler.KeyValue(value, 0));
                ClientHandler.putKey(key, value);
            } else if (opcode == 0xFA) { // AUX field
            	readLengthEncodedString(in); // key
                readLengthEncodedString(in); // value
                    // Discard both—just metadata
            } else if (opcode == 0xFB) { // RESIZEDB
                readLength(in); // number of keys hint
                readLength(in); // number of expires hint
            } else if (opcode == 0xFF) { // EOF
                break;
            } else {
                throw new IOException("Unknown opcode: " + opcode);
            }
        }
    }
    
    private static long readLength(DataInputStream in) throws IOException {
        int firstByte = in.readUnsignedByte();
        int type = (firstByte & 0xC0) >> 6;

        if (type == 0) {
            return firstByte & 0x3F;
        } else if (type == 1) {
            int secondByte = in.readUnsignedByte();
            return ((firstByte & 0x3F) << 8) | secondByte;
        } else if (type == 2) {
            return Integer.toUnsignedLong(in.readInt());
        } else {
            throw new IOException("Unsupported length encoding type");
        }
    }

    private static String readLengthEncodedString(DataInputStream in) throws IOException {
        long len = readLength(in);
        if (len < 0 || len > 512 * 1024) throw new IOException("Invalid string length: " + len);

        byte[] bytes = new byte[(int) len];
        in.readFully(bytes);
        return new String(bytes);
    }
}