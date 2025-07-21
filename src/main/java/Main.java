import java.io.*;
import java.net.*;
import java.util.*;

public class Main {
	private static ServerSocket serverSocket;
	private static List<String> bulkBuffer = new ArrayList<>();
	private static long replicationOffset = 0;

	public static void main(String[] args) {
		String masterHost = null;
		int masterPort = 0;
		int port = 6379;

		// Parse CLI flags or config
		for (int i = 0; i < args.length - 1; i++) {
			if (args[i].equals("--dir")) {
				Config.dir = args[i + 1];
			} else if (args[i].equals("--dbfilename")) {
				Config.dbFilename = args[i + 1];
			} else if (args[i].equals("--port")) {
				port = Integer.parseInt(args[i + 1]);
				Config.setPort(port);
			} else if (args[i].equals("--replicaof")) {
				String[] parts = args[i + 1].split(" ");
				if (parts.length == 2) {
					masterHost = parts[0];
					masterPort = Integer.parseInt(parts[1]);
					Config.setReplica(true);
				}
			}
		}

		// Load RDB file if configured
		if (!Config.dir.isEmpty() && !Config.dbFilename.isEmpty()) {
			String filePath = Config.dir + "/" + Config.dbFilename;
			File rdbFile = new File(filePath);
			if (rdbFile.exists()) {
				try (FileInputStream fis = new FileInputStream(rdbFile)) {
					RDBParser.loadFromStream(fis);
					System.out.println("Loaded RDB file: " + filePath);
				} catch (IOException e) {
					System.out.println("Failed to read RDB: " + e.getMessage());
				}
			} else {
				System.out.println("No RDB file found, starting with empty DB");
			}
		} else {
			System.out.println("No RDB config provided, starting with empty DB");
		}

		// Start replication thread if in replica mode
		if (Config.isReplica()) {
			String finalMasterHost = masterHost;
			int finalMasterPort = masterPort;
			new Thread(() -> connectToMaster(finalMasterHost, finalMasterPort)).start();
		}

		// Start client-facing server
		try {
			serverSocket = new ServerSocket();
			serverSocket.setReuseAddress(true);
			serverSocket.bind(new InetSocketAddress(port));
			System.out.println("Server is listening on port " + port);

			while (true) {
				Socket clientSocket = serverSocket.accept();
				System.out.println("Client connected.");
				new ClientHandler(clientSocket).start();
			}
		} catch (IOException e) {
			System.out.println("IOException: " + e.getMessage());
		}
	}

	private static void connectToMaster(String masterHost, int masterPort) {
        try {
            Socket masterSocket = new Socket(masterHost, masterPort);
            InputStream in = masterSocket.getInputStream();
            OutputStream out = masterSocket.getOutputStream();

            send(out, "*1\r\n$4\r\nPING\r\n");
            System.out.println("Sent PING to master");
            System.out.println("Received from master: " + readLine(in));

            String portStr = Integer.toString(Config.getPort());
            send(out, "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$" + portStr.length() + "\r\n" + portStr + "\r\n");
            System.out.println("Received from master: " + readLine(in));

            send(out, "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n");
            System.out.println("Received from master: " + readLine(in));

            send(out, "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n");
            System.out.println("Received: " + readLine(in));
            readLine(in);

            String rdbHeader = readLine(in);
            if (rdbHeader.startsWith("$")) {
                int rdbLength = Integer.parseInt(rdbHeader.substring(1));
                in.read(new byte[rdbLength]);
                readLine(in);
                System.out.println("Read " + rdbLength + " RDB bytes from master.");
            }

            new Thread(() -> {
                try {
                    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                    byte[] tmp = new byte[1024];
                    int read;
                    while ((read = in.read(tmp)) != -1) {
                        buffer.write(tmp, 0, read);
                        byte[] data = buffer.toByteArray();
                        int processed = processStream(data, out);
                        if (processed > 0 && processed <= data.length) {
                            buffer.reset();
                            buffer.write(data, processed, data.length - processed);
                        }
                    }
                } catch (IOException e) {
                    System.err.println("Replication stream error: " + e.getMessage());
                }
            }).start();

        } catch (IOException e) {
            System.err.println("Failed to connect to master: " + e.getMessage());
        }
    }

	private static int processStream(byte[] data, OutputStream out) {
	    try {
	        RespParser parser = new RespParser(data);
	        int lastPos = 0;

	        while (parser.hasNext()) {
	            RespCommand cmd = parser.next();
	            lastPos = parser.getPos();
	            replicationOffset += (lastPos - replicationOffset);

	            String[] arr = cmd.getArray();
	            String val = cmd.getValue();

	            if (arr != null && arr.length == 3 &&
	                "REPLCONF".equalsIgnoreCase(arr[0]) &&
	                "GETACK".equalsIgnoreCase(arr[1]) &&
	                "*".equals(arr[2])) {

	            	String offsetStr = Long.toString(replicationOffset);
	            	String ack = "*3\r\n" +
	            	             "$8\r\nREPLCONF\r\n" +
	            	             "$3\r\nACK\r\n" +
	            	             "$" + offsetStr.length() + "\r\n" +
	            	             offsetStr + "\r\n";
	                out.write(ack.getBytes("UTF-8"));
	                out.flush();
	                System.out.println("Sent ACK to master.");

	                bulkBuffer.clear();
	                continue;
	            }

	            if (arr != null) {
	                bulkBuffer.clear();
	                processCommand(cmd);
	                continue;
	            }

	            if (val != null) {
	                bulkBuffer.add(val);

	                if (bulkBuffer.size() == 3) {
	                    String[] complete = bulkBuffer.toArray(new String[0]);
	                    bulkBuffer.clear();

	                    if ("REPLCONF".equalsIgnoreCase(complete[0]) &&
	                        "GETACK".equalsIgnoreCase(complete[1]) &&
	                        "*".equals(complete[2])) {

	                        String ackResponse = "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n0\r\n";
	                        out.write(ackResponse.getBytes("UTF-8"));
	                        out.flush();
	                        System.out.println("Sent ACK to master.");
	                    } else {
	                        processCommand(new RespCommand(complete));
	                    }
	                }
	            }
	        }

	        return lastPos;
	    } catch (Exception e) {
	        System.err.println("Failed to process command: " + e.getMessage());
	        bulkBuffer.clear();
	        return 0;
	    }
	}


	private static String readLine(InputStream in) throws IOException {
		StringBuilder sb = new StringBuilder();
		int b;
		boolean cr = false;
		while ((b = in.read()) != -1) {
			if (cr && b == '\n')
				break;
			if (cr) {
				sb.append('\r');
				cr = false;
			}
			if (b == '\r')
				cr = true;
			else
				sb.append((char) b);
		}
		return sb.toString();
	}

	private static void processCommand(RespCommand command) {
		String[] elements = command.getArray();
		if (elements != null && elements.length > 0) {
			String cmd = elements[0].toUpperCase();
			if ("SET".equals(cmd)) {
				try {
					ClientHandler.handleSet(Arrays.asList(elements), null, true);
				} catch (IOException e) {
					System.err.println("Error applying propagated SET: " + e.getMessage());
				}
			}
		}
	}

	private static void send(OutputStream out, String data) throws IOException {
		out.write(data.getBytes("UTF-8"));
		out.flush();
	}
}