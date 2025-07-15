
class KeyValue {
        String value;
        long expirationTimestamp;

        KeyValue(String value, long expirationTimestamp) {
            this.value = value;
            this.expirationTimestamp = expirationTimestamp;
        }
        
        boolean hasExpired() {
            long now = System.currentTimeMillis();
            boolean expired = expirationTimestamp > 0 && now > expirationTimestamp;
            System.out.printf("Checking if expired: now=%d, expire=%d, expired=%b%n", now, expirationTimestamp, expired);
            return expired;
        }
        
        void updateExpiration(long newExpirationTimestamp) {
            this.expirationTimestamp = newExpirationTimestamp;
            System.out.println("Expiration updated: " + newExpirationTimestamp);
        }
        
}