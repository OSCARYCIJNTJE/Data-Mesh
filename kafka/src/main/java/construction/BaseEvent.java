package construction;

public class BaseEvent {

    public String eventId;
    public String eventType;
    public String siteId;
    public long timestamp;

    public BaseEvent() {}

    public BaseEvent(String eventId, String eventType, String siteId, long timestamp) {
        this.eventId = eventId;
        this.eventType = eventType;
        this.siteId = siteId;
        this.timestamp = timestamp;
    }
}
