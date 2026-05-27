package construction;

public class RiskEvent {

    public String riskId;
    public String siteId;
    public int riskScore;
    public String riskLevel;
    public String sourceType;     // NEW: correlation / delivery / equipment
    public String description;    // NEW: why this risk exists
    public long timestamp;

    public RiskEvent() {}

    public RiskEvent(
            String riskId,
            String siteId,
            int riskScore,
            String riskLevel,
            String sourceType,
            String description,
            long timestamp
    ) {
        this.riskId = riskId;
        this.siteId = siteId;
        this.riskScore = riskScore;
        this.riskLevel = riskLevel;
        this.sourceType = sourceType;
        this.description = description;
        this.timestamp = timestamp;
    }
}