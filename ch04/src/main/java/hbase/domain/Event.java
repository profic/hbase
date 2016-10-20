package hbase.domain;

/**
 * Created by cloudera on 10/20/16.
 */
public class Event {

    private long timestamp;
    private long value;

    public Event(long timestamp, long value) {
        this.timestamp = timestamp;
        this.value = value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }
}
