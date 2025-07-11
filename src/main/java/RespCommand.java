import java.util.Arrays;

class RespCommand {
    private String[] array;

    public RespCommand(String[] array) {
        this.array = array;
    }

    public String[] getArray() {
        return array;
    }
    
    @Override
    public String toString() {
        return "RespCommand{array=" + Arrays.toString(array) + "}";
    }

    public String getFirstArg() {
        if (array.length > 0) {
            return array[0];
        }
        return null;
    }
    
}