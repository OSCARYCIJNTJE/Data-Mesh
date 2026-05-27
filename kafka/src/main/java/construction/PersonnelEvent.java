package construction;

public class PersonnelEvent extends BaseEvent {

    public String personnelId;
    public String status; // INJURED / ABSENT
    public String reason;

    public PersonnelEvent() {}

    public PersonnelEvent(
            String eventId,
            String siteId,
            long timestamp,
            String personnelId,
            String status,
            String reason
    ) {
        super(eventId, "PERSONNEL_EVENT", siteId, timestamp);
        this.personnelId = personnelId;
        this.status = status;
        this.reason = reason;
    }
}
