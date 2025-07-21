import java.util.Arrays;

class RespCommand {
    private final String value;
    private final String[] array;

    public RespCommand(String value) {
        this.value = value;
        this.array = null;

    }

    public RespCommand(String[] array) {
    	this.value = null;
        this.array = array;
    }

    public boolean isSimple() {
        return array == null && value != null;
    }

    public String getValue() {
        return value;
    }

    public String[] getArray() {
        return array;
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
    
}