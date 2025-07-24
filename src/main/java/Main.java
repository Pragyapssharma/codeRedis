import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class Main {
	private static ServerSocket serverSocket;
	private static List<String> bulkBuffer = new ArrayList<>();
	private static long cumulativeOffset = 0;

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
			readLine(in);

			String portStr = Integer.toString(Config.getPort());
			send(out, "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$" + portStr.length() + "\r\n" + portStr + "\r\n");
			readLine(in);

			send(out, "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n");
			readLine(in);

			send(out, "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n");
			readLine(in);
			readLine(in);

			String rdbHeader = readLine(in);
			if (rdbHeader.startsWith("$")) {
				int rdbLength = Integer.parseInt(rdbHeader.substring(1));
				in.read(new byte[rdbLength]);
				readLine(in);
				System.out.println("Read " + rdbLength + " RDB bytes from master.");
				cumulativeOffset = 0;
			}

			new Thread(() -> {
				try {
					ByteArrayOutputStream buffer = new ByteArrayOutputStream();
					byte[] tmp = new byte[1024];
					int read;
					while ((read = in.read(tmp)) != -1) {
						buffer.write(tmp, 0, read);
						while (true) {
							byte[] data = buffer.toByteArray();
							int processed = processStream(data, out);

							if (processed == 0) {
//								buffer.reset();
								break;
							}
							if (processed > 0 && processed <= data.length) {
								buffer.reset();
								buffer.write(data, processed, data.length - processed);
							} else {
								buffer.reset();
								break;
							}
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
	        if (data.length == 0) return 0;
	        RespParser parser = new RespParser(data);
	        int totalConsumed = 0;

	        while (parser.hasNext()) {
	            int start = parser.getRawBytesRead();
	            RespCommand cmd = parser.next();
	            int end = parser.getRawBytesRead();
	            int segmentSize = end - start;

	            String[] arr = cmd.getArray();
	            String val = cmd.getValue();

	            // Check if the command is REPLCONF GETACK *
	            if (arr != null && arr.length == 3 && "REPLCONF".equalsIgnoreCase(arr[0]) && "GETACK".equalsIgnoreCase(arr[1]) && "*".equals(arr[2])) {
	                System.out.println("➡️ REPLCONF GETACK * detected");
	                respondWithAck(out);
	                cumulativeOffset += segmentSize;
	                totalConsumed += segmentSize;
	                continue;
	            }

	            // Handle bulk buffer commands
	            if (val != null) {
	                bulkBuffer.add(val);
	                if (bulkBuffer.size() == 3) {
	                    String a0 = bulkBuffer.get(0), a1 = bulkBuffer.get(1), a2 = bulkBuffer.get(2);
	                    if ("REPLCONF".equalsIgnoreCase(a0) && "GETACK".equalsIgnoreCase(a1) && "*".equals(a2)) {
	                        System.out.println("➡️ REPLCONF GETACK * detected (bulk)");
	                        respondWithAck(out);
	                        bulkBuffer.clear();
	                        cumulativeOffset += segmentSize;
	                        totalConsumed += segmentSize;
	                        continue;
	                    } else {
	                        processCommand(new RespCommand(bulkBuffer.toArray(new String[0])));
	                        bulkBuffer.clear();
	                    }
	                }
	                cumulativeOffset += segmentSize;
	                totalConsumed += segmentSize;
	                continue;
	            }

	            // Process other commands
	            processCommand(cmd);
	            cumulativeOffset += segmentSize;
	            totalConsumed += segmentSize;
	        }

	        return totalConsumed;
	    } catch (Exception e) {
	        System.err.println("Failed to process command: " + e.getMessage());
	        bulkBuffer.clear();
	        return 0;
	    }
	}
	
//	private static int processStream(byte[] data, OutputStream out) {
//	    try {
//	        if (data.length == 0) return 0;
//	        RespParser parser = new RespParser(data);
//	        int totalConsumed = 0;
//
//	        while (parser.hasNext()) {
//	            int start = parser.getRawBytesRead();
//	            RespCommand cmd = parser.next();
//	            int end = parser.getRawBytesRead();
//	            int segmentSize = end - start;
//
//	            String[] arr = cmd.getArray();
//	            String val = cmd.getValue();
//
//	            // 🧠 DEBUG: Raw byte segment info
//	            System.out.println("--------------------------------------------------");
//	            System.out.println("Parsed command bytes: start=" + start + ", end=" + end + ", segmentSize=" + segmentSize);
//	            System.out.println("Current cumulativeOffset BEFORE command: " + cumulativeOffset);
//
//	            if (val != null) {
//	                bulkBuffer.add(val);
//	                if (bulkBuffer.size() == 3) {
//	                    String a0 = bulkBuffer.get(0), a1 = bulkBuffer.get(1), a2 = bulkBuffer.get(2);
//	                    boolean isAck = "REPLCONF".equalsIgnoreCase(a0) &&
//	                                    "GETACK".equalsIgnoreCase(a1) &&
//	                                    "*".equals(a2);
//
//	                    System.out.println("Parsed bulk command: [" + a0 + ", " + a1 + ", " + a2 + "]");
//	                    if (isAck) {
//	                        System.out.println("➡️ REPLCONF GETACK * detected (bulk)");
//	                        respondWithAck(out);  // ✅ respond using cumulativeOffset before updating
//	                    } else {
//	                        processCommand(new RespCommand(bulkBuffer.toArray(new String[0])));
//	                    }
//
//	                    bulkBuffer.clear();
//	                    cumulativeOffset += segmentSize;  // ✅ update AFTER responding
//	                    totalConsumed += segmentSize;
//
//	                    System.out.println("Updated cumulativeOffset AFTER command: " + cumulativeOffset);
//	                    System.out.println("--------------------------------------------------");
//	                    continue;
//	                }
//	            }
//
//	            else if (arr != null) {
//	                boolean isAck = arr.length == 3 &&
//	                                "REPLCONF".equalsIgnoreCase(arr[0]) &&
//	                                "GETACK".equalsIgnoreCase(arr[1]) &&
//	                                "*".equals(arr[2]);
//
//	                System.out.println("Parsed array command: " + Arrays.toString(arr));
//	                if (isAck) {
//	                    System.out.println("➡️ REPLCONF GETACK * detected (array)");
//	                    respondWithAck(out);  // ✅ respond using current offset
//	                } else {
//	                    processCommand(cmd);
//	                }
//
//	                cumulativeOffset += segmentSize;
//	                totalConsumed += segmentSize;
//
//	                System.out.println("Updated cumulativeOffset AFTER command: " + cumulativeOffset);
//	                System.out.println("--------------------------------------------------");
//	                continue;
//	            }
//
//	            // Other command types (e.g. PING, SET)
//	            processCommand(cmd);
//	            cumulativeOffset += segmentSize;
//	            totalConsumed += segmentSize;
//
//	            System.out.println("Processed command: " + cmd);
//	            System.out.println("Updated cumulativeOffset AFTER command: " + cumulativeOffset);
//	            System.out.println("--------------------------------------------------");
//	        }
//
//	        return totalConsumed;
//	    } catch (Exception e) {
//	        System.err.println("Failed to process command: " + e.getMessage());
//	        bulkBuffer.clear();
//	        return 0;
//	    }
//	}
	
	private static void respondWithAck(OutputStream out) throws IOException {
	    String offset = Long.toString(cumulativeOffset);
	    String ack =
	        "*3\r\n" +
	        "$8\r\nREPLCONF\r\n" +
	        "$3\r\nACK\r\n" +
	        "$" + offset.length() + "\r\n" +
	        offset + "\r\n";
	    out.write(ack.getBytes("UTF-8"));
	    out.flush();
	    System.out.println("Sent REPLCONF ACK: " + offset);
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
		if (elements != null && elements.length > 0 && "SET".equalsIgnoreCase(elements[0])) {
			try {
				ClientHandler.handleSet(Arrays.asList(elements), null, true);
			} catch (IOException e) {
				System.err.println("Error applying propagated SET: " + e.getMessage());
			}
		}
	}

	private static void send(OutputStream out, String data) throws IOException {
		out.write(data.getBytes("UTF-8"));
		out.flush();
	}
}