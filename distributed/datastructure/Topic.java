package distributed.datastructure;

import java.io.Serializable;

public class Topic implements Serializable {
	private String groupName;
	public Topic(String groupName) {
		this.groupName = groupName;
	}

	public String getGroupName() {
		return groupName;
	}

	public void setGroupName(String groupName) {
		this.groupName = groupName;
	}

	@Override
	public String toString() {
		return groupName;
	}

	@Override
	public boolean equals(Object o) {
		if(o instanceof Topic) {
			Topic t = (Topic) o;
			return (getGroupName().equals(t.getGroupName()));
		}
		return false;
	}
}
