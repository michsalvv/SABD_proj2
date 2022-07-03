package kafka.queries.Windows;

import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import utils.Config;
import utils.Tools;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Implementation of a weekly custom window with a given timezone
 */
public class WeeklyWindow extends Windows<TimeWindow> {

    private final int startHour;
    private ZonedDateTime startTime;
    private ZonedDateTime endTime;
    private boolean first;
    private Timestamp lastWeek;

    public WeeklyWindow() {
        this.startHour = 0;
        this.first = true;
    }

    @Override
    public Map<Long, TimeWindow> windowsFor(final long timestamp) {
        final Map<Long, TimeWindow> windows = new LinkedHashMap<>();
        Timestamp currentWeek = Tools.getWeekSlot(new Timestamp(timestamp-Config.CEST));

        if (lastWeek != null && !currentWeek.equals(lastWeek)){
            first = true;
        }

        if (first) {
            first=false;
            final Instant instant = Instant.ofEpochMilli(timestamp);

            final ZonedDateTime zonedDateTime = instant.atZone(ZonedDateTime.now().getZone());
            startTime = zonedDateTime.getHour() >= startHour ? zonedDateTime.truncatedTo(ChronoUnit.DAYS).withHour(startHour) : zonedDateTime.truncatedTo(ChronoUnit.DAYS).minusDays(1).withHour(startHour);
            endTime = startTime.plusDays(7);
            lastWeek = currentWeek;
        }
        windows.put(toEpochMilli(startTime), new TimeWindow(toEpochMilli(startTime), toEpochMilli(endTime)));
        return windows;
    }

    @Override
    public long size() {
        return Duration.ofDays(7).toMillis();
    }

    @Override
    public long gracePeriodMs() {
        return 0;
    }

    private long toEpochMilli(final ZonedDateTime zonedDateTime) {
        return zonedDateTime.toInstant().toEpochMilli() + Config.CEST;
    }
}
