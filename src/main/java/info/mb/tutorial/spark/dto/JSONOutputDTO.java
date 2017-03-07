package info.mb.tutorial.spark.dto;

import java.io.Serializable;
/**
 *  
 * @author MBansal
 */
public class JSONOutputDTO implements Serializable {

	private static final long serialVersionUID = 1L;

	private Long numberOfUsersActive;
	private Long totalNumberOfEvents;
	private String groupId;
	private Long timestamp;
	public Long getNumberOfUsersActive() {
		return numberOfUsersActive;
	}
	public void setNumberOfUsersActive(Long numberOfUsersActive) {
		this.numberOfUsersActive = numberOfUsersActive;
	}
	public Long getTotalNumberOfEvents() {
		return totalNumberOfEvents;
	}
	public void setTotalNumberOfEvents(Long totalNumberOfEvents) {
		this.totalNumberOfEvents = totalNumberOfEvents;
	}
	public String getGroupId() {
		return groupId;
	}
	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}
	public Long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

}
