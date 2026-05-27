    package construction;

    public class CorrelationEvent {
        public String correlationId;
        public String siteId;
        public String type;
        public String severity;
        public String message;
        public String eventA;
        public String eventB;
        public long timestamp;

        public CorrelationEvent() {}

        public CorrelationEvent(
                String correlationId,
                String siteId,
                String type,
                String severity,
                String message,
                String eventA,
                String eventB,
                long timestamp
        ) {
            this.correlationId = correlationId;
            this.siteId = siteId;
            this.type = type;
            this.severity = severity;
            this.message = message;
            this.eventA = eventA;
            this.eventB = eventB;
            this.timestamp = timestamp;
        }
}
