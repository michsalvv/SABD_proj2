package utils.exception;

public class TemperatureOutOfBoundException extends Exception{
    public TemperatureOutOfBoundException(String errorMessage) {
        super(errorMessage);
    }
}