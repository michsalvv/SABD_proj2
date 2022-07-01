package kafka.queries.Windows;

import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import utils.Config;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.Map;

//Implementation of a daily custom window starting at given hour (like daily windows starting at 6pm) with a given timezone
public class MonthlyWindow extends Windows<TimeWindow> {

    private final int startHour;
    private ZonedDateTime startTime;
    private ZonedDateTime endTime;
    private boolean first;
    private Timestamp lastWeek;

    public MonthlyWindow() {
        this.startHour = 0;
        this.first = true;
    }

    @Override
    public Map<Long, TimeWindow> windowsFor(final long timestamp) {
        final Map<Long, TimeWindow> windows = new LinkedHashMap<>();

        if (first) {
//            System.out.println("FIRST HANDLER");
            first=false;
            final Instant instant = Instant.ofEpochMilli(timestamp);

            final ZonedDateTime zonedDateTime = instant.atZone(ZonedDateTime.now().getZone());
            startTime = zonedDateTime.getHour() >= startHour ? zonedDateTime.truncatedTo(ChronoUnit.DAYS).withHour(startHour) : zonedDateTime.truncatedTo(ChronoUnit.DAYS).minusDays(1).withHour(startHour);
            endTime = startTime.plusDays(31);
        }

        windows.put(toEpochMilli(startTime), new TimeWindow(toEpochMilli(startTime), toEpochMilli(endTime)));
        return windows;
    }

    @Override
    public long size() {
        return Duration.ofDays(31).toMillis();
    }

    @Override
    public long gracePeriodMs() {
        return 0;
    }

    private long toEpochMilli(final ZonedDateTime zonedDateTime) {
        return zonedDateTime.toInstant().toEpochMilli() + Config.CEST;
    }
}
