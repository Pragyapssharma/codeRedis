import java.util.Arrays;

class RespCommand {
    private String[] array;
    private RespCommand[] subCommands;

    public RespCommand(String[] array) {
        this.array = array;
        this.subCommands = null;
    }
    
    public RespCommand(RespCommand[] subCommands) {
        this.subCommands = subCommands;
        this.array = null; // No main array
    }

    public String[] getArray() {
        return array;
    }
    
    public RespCommand[] getSubCommands() {
        return subCommands;
    }
    
    @Override
    public String toString() {
    	if (array != null) {
            return "RespCommand{array=" + Arrays.toString(array) + "}";
        } else {
            return "RespCommand{subCommands=" + Arrays.toString(subCommands) + "}";
        }
    }

    public String getFirstArg() {
    	if (array != null && array.length > 0) {
            return array[0];
        } else if (subCommands != null && subCommands.length > 0) {
            return subCommands[0].getFirstArg();
        }
        return null;
    }
    
    public static String bulkString(String value) {
        if (value == null) {
            return "$-1\r\n"; // Null bulk string for missing keys
        }
        return "$" + value.length() + "\r\n" + value + "\r\n";
    }
    
}