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
            } else if (opcode == 0xFF) { // EOF
                break;
            } else {
                throw new IOException("Unknown opcode: " + opcode);
            }
        }
    }

    private static String readLengthEncodedString(DataInputStream in) throws IOException {
        int len = in.readUnsignedByte();
        byte[] bytes = new byte[len];
        in.readFully(bytes);
        return new String(bytes);
    }
}