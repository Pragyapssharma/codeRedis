
public class Config {
	
	public static String dir = "";
    public static String dbFilename = "";
    public static int port = 6379;
    public static boolean isReplica = false;
    
    public static final String masterReplId = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
    public static final long masterReplOffset = 0;
    
    
    public static String getDir() { return dir; }
    public static void setDir(String dir) { Config.dir = dir; }

    public static String getDbFilename() { return dbFilename; }
    public static void setDbFilename(String dbFilename) { Config.dbFilename = dbFilename; }

    public static int getPort() { return port; }
    public static void setPort(int port) { Config.port = port; }

    public static boolean isReplica() { return isReplica; }
    public static void setReplica(boolean isReplica) { Config.isReplica = isReplica; }

}
