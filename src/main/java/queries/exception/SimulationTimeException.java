package queries.exception;

public class MalformedTimeException extends Exception{
    public MalformedTimeException(String errorMessage) {
        super(errorMessage);
    }
}
