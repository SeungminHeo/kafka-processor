package model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

public class ClickMatrix {
    private String piwikId;
    private String itemId;
    private Long clickCount;

    public ClickMatrix() {}

    public ClickMatrix(String piwikId, String itemId) {
        this.piwikId = piwikId;
        this.itemId = itemId;
    }

    public static Builder builder() { return new Builder(); }

    public static Builder builder(ClickLogs clickLogs) {
        return new Builder(clickLogs);
    }

    public static ClickMatrix from(ClickLogs clickLogs) {
        return new ClickMatrix(clickLogs.getPiwikId(), clickLogs.getItemId());
    }

    public String getItemId() {
        return itemId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    public String getPiwikId() {
        return piwikId;
    }

    public Long getClickCount() {
        return this.clickCount;
    }

    public void setClickCount(Long clickCount) {
        this.clickCount = clickCount;
    }

    public void setPiwikId(String piwikId) {
        this.piwikId = piwikId;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> raw = new HashMap<>();
        raw.put("piwikId", this.piwikId);
        raw.put("itemId", this.itemId);
        raw.put("clickCount", this.clickCount);
        return raw;
    }

    public String toKeyString() {
        return this.piwikId + "," + this.itemId;
    }

    @Override
    public String toString() {
        try {
            return new ObjectMapper().writeValueAsString(this.toMap());
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        }
    }

    private static final class Builder {
        private String piwikId;
        private String itemId;
        private Long clickCount;

        private Builder() {}

        private Builder(ClickLogs clickLogs) {
            this.piwikId = clickLogs.getPiwikId();
            this.itemId = clickLogs.getItemId();
        }

        public Builder piwikId(String value) {
            this.piwikId = value;
            return this;
        }

        public Builder itemId(String value) {
            this.itemId = value;
            return this;
        }

    }
}
