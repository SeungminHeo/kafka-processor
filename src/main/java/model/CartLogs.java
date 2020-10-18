package model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public class CartLogs {
    private String piwikId;
    @JsonFormat(shape=JsonFormat.Shape.STRING, pattern="yyyy-MM-dd HH:mm:ss")
    private Timestamp time;
    private int visitCount;
    private String isApp;
    private String isMobile;
    private String title;
    private String url;
    private String urlref;
    private Date dateId;

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(Logs logs) { return new Builder(logs); }

    public CartLogs() { }

    public CartLogs(String piwikId, Timestamp time, int visitCount, String isApp, String isMobile, String title, String url, String urlref, Date dateId) {
        this.piwikId = piwikId;
        this.time = time;
        this.visitCount = visitCount;
        this.isApp = isApp;
        this.isMobile = isMobile;
        this.title = title;
        this.url = url;
        this.urlref = urlref;
        this.dateId = dateId;
    }

    public CartLogs(Builder builder) {
        piwikId = builder.piwikId;
        time = builder.time;
        visitCount = builder.visitCount;
        isApp = builder.isApp;
        isMobile = builder.isMobile;
        title = builder.title;
        url = builder.url;
        urlref = builder.urlref;
        dateId = builder.dateId;
    }

    public CartLogs(CartLogs copy) {
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
    }

    public String getPiwikId() {
        return piwikId;
    }

    public void setPiwikId(String piwikId) { this.piwikId = piwikId; }

    public Timestamp getTime() { return time; }

    public void setTime(Timestamp time) {
        this.time = time;
    }

    public int getVisitCount() {
        return visitCount;
    }

    public void setVisitCount(int visitCount) {
        this.visitCount = visitCount;
    }

    public String getIsApp() {
        return isApp;
    }

    public void setIsApp(String isApp) {
        this.isApp = isApp;
    }

    public String getIsMobile() {
        return isMobile;
    }

    public void setIsMobile(String isMobile) {
        this.isMobile = isMobile;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUrlref() {
        return urlref;
    }

    public void setUrlref(String urlref) {
        this.urlref = urlref;
    }

    public Date getDateId() {
        return dateId;
    }

    public void setDateId(Date dateId) {
        this.dateId = dateId;
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
        private Timestamp time;
        private int visitCount;
        private String isApp;
        private String isMobile;
        private String title;
        private String url;
        private String urlref;
        private Date dateId;


        private Builder() {
        }

        private Builder(Logs logs) {
            this.piwikId = logs.getPiwikId();
            this.time = logs.getTime();
            this.visitCount = logs.getVisitCount();
            this.isApp = logs.getIsApp();
            this.isMobile = logs.getIsMobile();
            this.title = logs.getTitle();
            this.url = logs.getUrl();
            this.urlref = logs.getUrlref();
            this.dateId = logs.getDateId();
        }

        public Builder piwikId(String value) {
            piwikId = value;
            return this;
        }

        public Builder time(Timestamp value) {
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

        public CartLogs build() {
            return new CartLogs(this);
        }
    }
}