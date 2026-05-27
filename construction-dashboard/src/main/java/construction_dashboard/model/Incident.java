package construction_dashboard.model;

public class Incident {

    public String type;
    public String severity;
    public String message;

    public Incident() {}

    public Incident(String type, String severity, String message) {
        this.type = type;
        this.severity = severity;
        this.message = message;
    }
}