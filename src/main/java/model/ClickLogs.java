package model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.sql.Date;
import java.sql.Time;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class ClickLogs {
    private String piwikId;
    private Time time;
    private int visitCount;
    private String isApp;
    private String isMobile;
    private String title;
    private String url;
    private String urlref;
    private Date dateId;
    private String itemId;
    private String categoryId;


    public static Builder builder() {
        return new Builder();
    }

    public ClickLogs(String piwikId, Time time, int visitCount, String isApp, String isMobile, String title,
                            String url, String urlref, Date dateId, String itemId, String categoryId) {
        this.piwikId = piwikId;
        this.time = time;
        this.visitCount = visitCount;
        this.isApp = isApp;
        this.isMobile = isMobile;
        this.title = title;
        this.url = url;
        this.urlref = urlref;
        this.dateId = dateId;
        this.itemId = itemId;
        this.categoryId = categoryId;
    }

    public ClickLogs(Builder builder) {
        piwikId = builder.piwikId;
        time = builder.time;
        visitCount = builder.visitCount;
        isApp = builder.isApp;
        isMobile = builder.isMobile;
        title = builder.title;
        url = builder.url;
        urlref = builder.urlref;
        dateId = builder.dateId;
        itemId = builder.itemId;
        categoryId = builder.categoryId;
    }

    public ClickLogs(ClickLogs copy) {
        Builder builder = new Builder();
        builder.piwikId = copy.piwikId;
        builder.time = copy.time;
        builder.visitCount = copy.visitCount;
        builder.isApp = copy.isApp;
        builder.isMobile = copy.isMobile;
        builder.title = copy.title;
        builder.url = copy.url;
        builder.urlref = copy.urlref;
        builder.dateId = copy.dateId;
        builder.itemId = copy.itemId;
        builder.categoryId = copy.categoryId;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> raw = new HashMap<>();
        raw.put("piwikId", this.piwikId);
        raw.put("time", this.time.toString());
        raw.put("visitCount", this.visitCount);
        raw.put("isApp", this.isApp);
        raw.put("isMobile", this.isMobile);
        raw.put("title", this.title);
        raw.put("url", this.url);
        raw.put("urlref", this.urlref);
        raw.put("dateId", this.dateId.toString());
        raw.put("itemId", this.itemId);
        raw.put("categoryId", this.categoryId);
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
        private String piwikId;
        private Time time;
        private int visitCount;
        private String isApp;
        private String isMobile;
        private String title;
        private String url;
        private String urlref;
        private Date dateId;
        private String itemId;
        private String categoryId;


        private Builder() {
        }

        public Builder piwikId(String value) {
            piwikId = value;
            return this;
        }

        public Builder time(Time value) {
            time = value;
            return this;
        }

        public Builder visitCount(int value) {
            visitCount = value;
            return this;
        }

        public Builder isApp(String value) {
            isApp = value;
            return this;
        }

        public Builder isMobile(String value) {
            isMobile = value;
            return this;
        }

        public Builder title(String value) {
            title = value;
            return this;
        }

        public Builder url(String value) {
            url = value;
            return this;
        }

        public Builder urlref(String value) {
            urlref = value;
            return this;
        }

        public Builder dateId(Date value) {
            dateId = value;
            return this;
        }

        public Builder preprocess() {
            Objects.requireNonNull(url, "Url is not set, null is not allowed.");
            // parsing itemid from click_log
            if (url.contains("branduid=")) {
                this.itemId = url.split("branduid=")[1].split("&")[0];
            } else {
                this.itemId = null;
            }
            // parsing item categoryId from click_log
            if (url.contains("xcode=")) {
                this.categoryId = url.split("xcode=")[1].split("&")[0];
            } else {
                this.categoryId = null;
            }
            return this;
        }

        public ClickLogs build() {
            return new ClickLogs(this);
        }
    }
}
