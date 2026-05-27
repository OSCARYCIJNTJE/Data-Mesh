package construction;

public class DeliveryEvent extends BaseEvent {

    public String material;
    public int quantity;
    public String status; // DELIVERED / DELAYED

    public DeliveryEvent() {}

    public DeliveryEvent(
            String eventId,
            String siteId,
            long timestamp,
            String material,
            int quantity,
            String status
    ) {
        super(eventId, "DELIVERY_EVENT", siteId, timestamp);
        this.material = material;
        this.quantity = quantity;
        this.status = status;
    }
}