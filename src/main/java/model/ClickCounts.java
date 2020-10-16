package model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

public class ClickCounts {
    private String itemId;
    private Long clickCount;

    public ClickCounts() {}

    public ClickCounts(String itemId) {
        this.itemId = itemId;
    }

    public static Builder builder() { return new Builder(); }

    public static Builder builder(ClickLogs clickLogs) { return new Builder(clickLogs); }

    public static ClickCounts from(ClickLogs clickLogs) {
        return new ClickCounts(clickLogs.getItemId());
    }

    public String getItemId() {
        return itemId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    public Long getClickCount() {
        return clickCount;
    }

    public void setClickCount(Long clickCount) {
        this.clickCount = clickCount;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> raw = new HashMap<>();
        raw.put("itemId", this.itemId);
        raw.put("clickCount", this.clickCount);
        return raw;
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

    public static final class Builder {
        private String itemId;
        private int clickCount;

        private Builder() {}

        private Builder(ClickLogs clickLogs) {
            itemId = clickLogs.getItemId();
        }

        public Builder itemId(String value){
            this.itemId = value;
            return this;
        }


    }
}
