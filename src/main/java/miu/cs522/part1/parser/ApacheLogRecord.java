package miu.cs522.part1.parser;

import miu.cs522.part1.builder.IBuilder;

/**
 * @author XIII
 */
public class ApacheLogRecord {

    private String ipAddress;
    private Integer bytesSent;

    private ApacheLogRecord(Builder builder) {
        this.ipAddress = builder.ipAddress;
        this.bytesSent = builder.bytesSent;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public Integer getBytesSent() {
        return bytesSent;
    }

    public static class Builder implements IBuilder<ApacheLogRecord> {

        private String ipAddress;
        private Integer bytesSent;

        public Builder withIpAddress(String ipAddress) {
            this.ipAddress = ipAddress;
            return this;
        }

        public Builder withBytesSent(Integer bytesSent) {
            this.bytesSent = bytesSent;
            return this;
        }

        @Override
        public ApacheLogRecord build() {
            return new ApacheLogRecord(this);
        }
    }
}
