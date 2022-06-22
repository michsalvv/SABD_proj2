package queries.exception;

public class NegativeTemperatureException extends Exception{
    public NegativeTemperatureException(String errorMessage) {
        super(errorMessage);
    }
}