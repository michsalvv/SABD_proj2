package utils.tuples;

//* ts, cell_0, avg_temp0, med_temp0, ...
//        * cell_15, avg_temp15, med_temp15

import java.sql.Timestamp;
import java.util.List;

public class OutputQ3 implements OutputQuery {
    String header = buildHeader();

    private static final String delimiter = ";";
    private List<ValQ3> row;

    @Override
    public String toCSV() {
        Timestamp timeslot = row.get(0).getTimestamp();
        StringBuilder builder = new StringBuilder();
        builder.append(timeslot.toString()).append(delimiter);
        for (ValQ3 val :row) {
            builder.append(val.getCell_id()).append(delimiter);
            builder.append(val.getMean_temp()).append(delimiter);
            builder.append(val.getMedian_temp()).append(delimiter);
        }

        // Simply remove last useless delimiter
        if (builder.length() > 0) {
            builder.setLength(builder.length() - 1);
        }

        return builder.toString();
    }

    @Override
    public String getCSVHeader() {
        return header;
    }

    public OutputQ3(List<ValQ3> row) {
        this.row = row;
    }

    @Override
    public String toString() {
        return "OutputQ3{" +
                "header='" + header + '\'' +
                ", row=" + row +
                '}';
    }

    public static String buildHeader() {
        String header = "ts;";
        for (int i = 0; i < 16; i++) {
            header = header + String.format("cell_%d;avg_temp%d;med_temp%d;",i,i,i);
        }
        return header;
    }
}
