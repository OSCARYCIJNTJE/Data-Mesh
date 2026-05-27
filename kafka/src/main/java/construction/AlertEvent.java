package construction;

public class AlertEvent {

    public String alertType;
    public String siteId;
    public String severity;
    public long count;
    public long timestamp;

    public AlertEvent() {
    }

    public AlertEvent(
            String alertType,
            String siteId,
            String severity,
            long count,
            long timestamp
    ) {
        this.alertType = alertType;
        this.siteId = siteId;
        this.severity = severity;
        this.count = count;
        this.timestamp = timestamp;
    }
}