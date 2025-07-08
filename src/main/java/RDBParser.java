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
			int b;

			try {
				b = in.readUnsignedByte();
			} catch (IOException e) {
				break; // End of stream
			}

			switch (b) {
			case 0xFD:
				expireAtMillis = readUnsignedLong(in); // 64-bit expiry time in ms (absolute)
				System.out.printf("Raw expireAtMillis before conversion: %d\n", expireAtMillis);
				if (expireAtMillis < 2_000_000_000_000L) { // less than ~Jan 2033 in ms?
					// sometimes expiry can be in seconds? rare for 64-bit, but add safeguard
					expireAtMillis *= 1000L;
					System.out.printf("After multiplying by 1000: %d\n", expireAtMillis);
				}
				hasExpiry = true;
				break;
			case 0xFC:
				byte[] expiryBytes = new byte[4];
				in.readFully(expiryBytes);
				long expirySeconds = ((expiryBytes[0] & 0xFFL) << 24) | ((expiryBytes[1] & 0xFFL) << 16)
						| ((expiryBytes[2] & 0xFFL) << 8) | (expiryBytes[3] & 0xFFL);
				expireAtMillis = expirySeconds * 1000L; // convert to ms absolute timestamp
				System.out.printf("Expiry (0xFC) seconds: %d, millis: %d\n", expirySeconds, expireAtMillis);
				hasExpiry = true;
				break;

			case 0xFE:
				in.readByte();
				break;
			case 0xFA:
				readLengthEncodedString(in);
				readLengthEncodedString(in);
				break;
			case 0xFB:
				readLength(in);
				readLength(in);
				break;
			case 0xFF:
				return;
			default:
				if (b >= 0x00 && b <= 0x06) {
					// Assume `b` is a data type (e.g. string = 0x00, list = 0x01, etc.)
					// For this challenge, only strings are supported.
					String key = readLengthEncodedString(in);
					String value = readLengthEncodedString(in);

					if (hasExpiry) {
					    long now = System.currentTimeMillis();
					    System.out.printf("Checking expiry for key '%s': expireAtMillis=%d (%s), now=%d (%s)%n",
					            key, expireAtMillis, new java.util.Date(expireAtMillis), now, new java.util.Date(now));
					    if (expireAtMillis != 0 && expireAtMillis < now) {
					        System.out.println("Skipping expired key: " + key);
					    } else {
					        ClientHandler.putKeyWithExpiry(key, value, expireAtMillis);
					        System.out.println("Loaded key with expiry: " + key);
					    }
					    hasExpiry = false;
					    expireAtMillis = 0;
					} else {
					    ClientHandler.putKeyWithExpiry(key, value, 0);
					    System.out.println("Loaded key without expiry: " + key);
					}
				} else {
					System.out.println("Skipping unsupported type: " + b);
					skipUnsupportedObject(b, in);
				}
				break;

			}
		}

	}

	private static void skipUnsupportedObject(int objectType, DataInputStream in) throws IOException {
		System.out.println("Skipping unsupported type: " + objectType);

		// For now, let's assume these unsupported types encode a key and value as
		// length-encoded strings
		// so we try to read and discard both.
		try {
			readLengthEncodedString(in); // Skip key
			readLengthEncodedString(in); // Skip value or payload
		} catch (IOException e) {
			throw new IOException("Failed to skip unsupported object type: " + objectType, e);
		}
	}

	private static long readUnsignedLong(DataInputStream in) throws IOException {
		long value = 0;
		for (int i = 0; i < 8; i++) {
			value = (value << 8) | (in.readUnsignedByte() & 0xFF);
		}
		return value;
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
			case 0:
				return in.readByte(); // 8-bit int
			case 1:
				return in.readShort(); // 16-bit int
			case 2:
				return in.readInt(); // 32-bit int
			default:
				throw new IOException("Unsupported encoded length type: " + encType);
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
			case 0:
				return String.valueOf(in.readByte()); // 8-bit int
			case 1:
				return String.valueOf(in.readShort()); // 16-bit int
			case 2:
				return String.valueOf(in.readInt()); // 32-bit int
			default:
				throw new IOException("Unsupported encoded string type: " + encType);
			}
		} else {
			throw new IOException("Invalid length prefix");
		}
	}
}