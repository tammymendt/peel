package eu.stratosphere.peel.analyser.model;

import javax.persistence.*;
import java.util.Date;

/**
 * A TaskInstanceEvent is a event of a taskInstance. Every taskInstance has a name
 * and a value that can be of the type int, double, String and Date.
 * Created by Fabian on 01.11.14.
 */
@Entity
public class TaskInstanceEvents {
    private Integer EventID;
    private String eventName;
    private Integer valueInt;
    private Double valueDouble;
    private Date valueTimestamp;
    private String valueVarchar;
    private TaskInstance taskInstance;

    public TaskInstanceEvents() {
    }

    @Id
    @GeneratedValue
    public Integer getEventID() {
        return EventID;
    }

    public void setEventID(Integer eventID) {
        this.EventID = eventID;
    }

    @Column
    public String getEventName() {
        return eventName;
    }

    public void setEventName(String eventName) {
        this.eventName = eventName;
    }

    @Column
    public Integer getValueInt() {
        return valueInt;
    }

    public void setValueInt(Integer valueInt) {
        this.valueInt = valueInt;
    }

    @Column
    public Double getValueDouble() {
        return valueDouble;
    }

    public void setValueDouble(Double valueDouble) {
        this.valueDouble = valueDouble;
    }

    @Column(columnDefinition = "TIMESTAMP")
    public Date getValueTimestamp() {
        return valueTimestamp;
    }

    public void setValueTimestamp(Date valueTimestamp) {
        this.valueTimestamp = valueTimestamp;
    }

    @Column
    public String getValueVarchar() {
        return valueVarchar;
    }

    public void setValueVarchar(String valueVarchar) {
        this.valueVarchar = valueVarchar;
    }

    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn
    public TaskInstance getTaskInstance() {
        return taskInstance;
    }

    public void setTaskInstance(TaskInstance taskInstance) {
        this.taskInstance = taskInstance;
    }
}
