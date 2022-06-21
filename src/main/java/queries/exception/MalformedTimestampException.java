package queries.flink.exception;

public class MalformedTimestampException extends Exception{
    public MalformedTimestampException(String errorMessage) {
        super(errorMessage);
    }
}
