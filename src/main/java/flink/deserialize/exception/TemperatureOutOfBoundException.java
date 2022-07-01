package flink.deserialize.exception;

public class TemperatureOutOfBoundException extends Exception{
    public TemperatureOutOfBoundException(String errorMessage) {
        super(errorMessage);
    }
}