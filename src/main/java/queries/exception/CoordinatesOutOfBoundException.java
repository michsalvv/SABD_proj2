package queries.exception;

public class CoordinatesOutOfBoundException extends Exception{
    public CoordinatesOutOfBoundException(String errorMessage) {
        super(errorMessage);
    }
}