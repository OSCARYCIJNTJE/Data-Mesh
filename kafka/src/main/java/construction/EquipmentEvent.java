package construction;

public class EquipmentEvent extends BaseEvent {

    public String equipmentId;
    public String status; // OK / MALFUNCTION
    public String issue;

    public EquipmentEvent() {}

    public EquipmentEvent(
            String eventId,
            String siteId,
            long timestamp,
            String equipmentId,
            String status,
            String issue
    ) {
        super(eventId, "EQUIPMENT_EVENT", siteId, timestamp);
        this.equipmentId = equipmentId;
        this.status = status;
        this.issue = issue;
    }
}