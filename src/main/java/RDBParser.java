import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

public class RDBParser {
    public static void loadFromStream(InputStream stream) throws IOException {
    	
        DataInputStream in = new DataInputStream(stream);
        long expireAtMillis = 0;
        boolean hasExpiry = false;


        // Read header: "REDIS" + version (magic 5 bytes + 4 version bytes)
        byte[] header = new byte[9];
        in.readFully(header);

        while (true) {
            int b = in.readUnsignedByte();

            switch (b) {
                case 0xFD:
                	expireAtMillis = in.readLong();
                    hasExpiry = true;
                    break;
                case 0xFC:
                	expireAtMillis = ((long) in.readInt()) * 1000;
                    hasExpiry = true;
                    break;
                case 0xFE:
                    in.readByte(); break;
                case 0xFA:
                    readLengthEncodedString(in); readLengthEncodedString(in); break;
                case 0xFB:
                    readLength(in); readLength(in); break;
                case 0xFF:
                    return;
                default:
                    // Assume `b` is a data type (e.g. string = 0x00, list = 0x01, etc.)
                    // For this challenge, only strings are supported.
                	String key = readLengthEncodedString(in);
                	String value = readLengthEncodedString(in);
                	long now = System.currentTimeMillis();
                	if (hasExpiry) {
                		if (expireAtMillis >= System.currentTimeMillis()) {
                            // If the key hasn't expired, insert it with expiry time
                            ClientHandler.putKeyWithExpiry(key, value, expireAtMillis);
                        } else {
                            // Skip inserting expired key
                            System.out.println("Skipping expired key: " + key);
                        }
                        hasExpiry = false;
                        expireAtMillis = 0;
                    } else {
                        // No expiry, just insert the key-value pair
                        ClientHandler.putKeyWithExpiry(key, value, 0);
                    }
                	break;
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
        } else if (type == 3) {
            int encType = firstByte & 0x3F;
            switch (encType) {
                case 0: return in.readByte();       // 8-bit int
                case 1: return in.readShort();      // 16-bit int
                case 2: return in.readInt();        // 32-bit int
                default: throw new IOException("Unsupported encoded length type: " + encType);
            }
        } else {
            throw new IOException("Invalid length prefix");
        }
    }

    private static String readLengthEncodedString(DataInputStream in) throws IOException {
        int firstByte = in.readUnsignedByte();
        int type = (firstByte & 0xC0) >> 6;

        if (type == 0) {
            int len = firstByte & 0x3F;
            byte[] bytes = new byte[len];
            in.readFully(bytes);
            return new String(bytes);
        } else if (type == 1) {
            int secondByte = in.readUnsignedByte();
            int len = ((firstByte & 0x3F) << 8) | secondByte;
            byte[] bytes = new byte[len];
            in.readFully(bytes);
            return new String(bytes);
        } else if (type == 2) {
            int len = in.readInt();
            byte[] bytes = new byte[len];
            in.readFully(bytes);
            return new String(bytes);
        } else if (type == 3) {
            int encType = firstByte & 0x3F;
            switch (encType) {
                case 0: return String.valueOf(in.readByte());   // 8-bit int
                case 1: return String.valueOf(in.readShort());  // 16-bit int
                case 2: return String.valueOf(in.readInt());    // 32-bit int
                default:
                    throw new IOException("Unsupported encoded string type: " + encType);
            }
        } else {
            throw new IOException("Invalid length prefix");
        }
    }
}