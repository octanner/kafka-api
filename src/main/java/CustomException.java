
public class CustomException extends Exception {

    public CustomException(String message) {
        super("status~500~"+message);
    }

}