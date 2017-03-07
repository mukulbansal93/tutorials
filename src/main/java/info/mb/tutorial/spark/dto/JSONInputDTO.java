package info.mb.tutorial.spark.dto;

import java.io.Serializable;
/**
 *  
 * @author MBansal
 */
public class JSONInputDTO implements Serializable{

	private static final long serialVersionUID = 1L;
	
	private String id;
	private String userId;
	private String groupId;
	private Long timestamp;
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getUserId() {
		return userId;
	}
	public void setUserId(String userId) {
		this.userId = userId;
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
