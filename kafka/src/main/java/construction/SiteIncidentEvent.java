package construction;

public class SiteIncidentEvent {

    public String incidentId;
    public String siteId;
    public long timestamp;
    public String type;
    public String severity;
    public String message;

    public SiteIncidentEvent() {}

    public SiteIncidentEvent(
            String incidentId,
            String siteId,
            long timestamp,
            String type,
            String severity,
            String message
    ) {
        this.incidentId = incidentId;
        this.siteId = siteId;
        this.timestamp = timestamp;
        this.type = type;
        this.severity = severity;
        this.message = message;
    }
}
