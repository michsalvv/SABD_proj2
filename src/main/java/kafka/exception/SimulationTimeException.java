package kafka.exception;

public class SimulationTimeException extends Exception{
    public SimulationTimeException(String errorMessage) {
        super(errorMessage);
    }
}
