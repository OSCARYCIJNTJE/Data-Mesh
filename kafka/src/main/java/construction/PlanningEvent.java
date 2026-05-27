package construction;

public class PlanningEvent extends BaseEvent {

    public String changeType;   // SCHEDULE_CHANGE / DESIGN_CHANGE
    public String description;

    public PlanningEvent() {}

    public PlanningEvent(
            String eventId,
            String siteId,
            long timestamp,
            String changeType,
            String description
    ) {
        super(eventId, "PLANNING_EVENT", siteId, timestamp);
        this.changeType = changeType;
        this.description = description;
    }
}
