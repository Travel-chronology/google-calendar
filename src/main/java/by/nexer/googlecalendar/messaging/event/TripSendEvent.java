package by.nexer.googlecalendar.messaging.event;

import java.util.Objects;

public class TripSendEvent {
    private String summary;
    private String description;
    private String startDate;
    private String endDate;

    public TripSendEvent() {
    }

    public TripSendEvent(String summary, String description, String startDate, String endDate) {
        this.summary = summary;
        this.description = description;
        this.startDate = startDate;
        this.endDate = endDate;
    }

    public String getSummary() {
        return summary;
    }

    public void setSummary(String summary) {
        this.summary = summary;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getStartDate() {
        return startDate;
    }

    public void setStartDate(String startDate) {
        this.startDate = startDate;
    }

    public String getEndDate() {
        return endDate;
    }

    public void setEndDate(String endDate) {
        this.endDate = endDate;
    }

    @Override
    public String toString() {
        return "TripSendEvent{" +
                "summary='" + summary + '\'' +
                ", description='" + description + '\'' +
                ", startDate='" + startDate + '\'' +
                ", endDate='" + endDate + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TripSendEvent that = (TripSendEvent) o;
        return Objects.equals(summary, that.summary) &&
                Objects.equals(description, that.description) &&
                Objects.equals(startDate, that.startDate) &&
                Objects.equals(endDate, that.endDate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(summary, description, startDate, endDate);
    }
}
