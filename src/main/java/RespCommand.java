import java.util.Arrays;

class RespCommand {
//    private final String[] array;
	private final String value;
    private RespCommand[] array;

    public RespCommand(String value) {
        this.value = value;
        this.array = null;
    }
    
    public RespCommand(RespCommand[] array) {
        this.array = array;
        this.value = null; // No main array
    }
    
    public String getValue() {
        return value;
    }

//    public String[] getArray() {
//        return array;
//    }
    
//    public RespCommand[] getSubCommands() {
//        return subCommands;
//    }
    
    public String[] getArray() {
        if (array == null) return null;

        String[] result = new String[array.length];
        for (int i = 0; i < array.length; i++) {
            result[i] = array[i].getValue();
        }
        return result;
    }
    
    public boolean isSimple() {
        return value != null;
    }
    
    @Override
    public String toString() {
        if (isSimple()) {
            return "RespCommand{value=" + value + "}";
        } else if (array != null) {
            return "RespCommand{subCommands=" + Arrays.toString(array) + "}";
        } else {
            return "RespCommand(empty)";
        }
    }
    
//    public String toString() {
//    	if (array != null) {
//            return "RespCommand{array=" + Arrays.toString(array) + "}";
//        } else if (subCommands != null) {
//            return "RespCommand{subCommands=" + Arrays.toString(subCommands) + "}";
//        } else {
//            return "RespCommand(empty)";
//        }
//    }

//    public String getFirstArg() {
//    	if (array != null && array.length > 0) {
//            return array[0];
//        } else if (subCommands != null && subCommands.length > 0) {
//            return subCommands[0].getFirstArg();
//        }
//        return null;
//    }
//    
//    public static String bulkString(String value) {
//        if (value == null) {
//            return "$-1\r\n"; // Null bulk string for missing keys
//        }
//        return "$" + value.length() + "\r\n" + value + "\r\n";
//    }
//    
//    public static String mset(String[] keys, String[] values) {
//        if (keys.length != values.length) {
//            throw new IllegalArgumentException("Keys and values arrays must be of the same length.");
//        }
//        
//        StringBuilder command = new StringBuilder();
//        command.append("*").append(keys.length * 2 + 1).append("\r\n");
//        command.append("$4\r\nMSET\r\n");
//
//        for (int i = 0; i < keys.length; i++) {
//            command.append("$").append(keys[i].length()).append("\r\n").append(keys[i]).append("\r\n");
//            command.append("$").append(values[i].length()).append("\r\n").append(values[i]).append("\r\n");
//        }
//        
//        return command.toString();
//    }
//
//    public static String hset(String key, String[] fields, String[] values) {
//        if (fields.length != values.length) {
//            throw new IllegalArgumentException("Fields and values arrays must be of the same length.");
//        }
//
//        StringBuilder command = new StringBuilder();
//        command.append("*").append(fields.length * 2 + 2).append("\r\n");
//        command.append("$4\r\nHSET\r\n");
//        command.append("$").append(key.length()).append("\r\n").append(key).append("\r\n");
//
//        for (int i = 0; i < fields.length; i++) {
//            command.append("$").append(fields[i].length()).append("\r\n").append(fields[i]).append("\r\n");
//            command.append("$").append(values[i].length()).append("\r\n").append(values[i]).append("\r\n");
//        }
//        
//        return command.toString();
//    }
    
}