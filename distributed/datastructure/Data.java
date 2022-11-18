package distributed.datastructure;
import java.io.File;  // Import the File class


import java.io.Serializable;
import java.util.Objects;

public class Data implements Serializable {
	private String username;
	private String filepath;
	private Topic key;
	private Value value;

	private File attached_file;

	public Data(String username, Topic key,Value value, String filepath) {
		this.username = username;
		this.key = key;
		this.value = value;
		this.filepath = filepath;

		if(!filepath.equals("")) {
			try {
				this.attached_file = new File(filepath);
			} catch (Exception e) {
				System.out.println("An error occurred.");
				e.printStackTrace();
			}
		}
	}
	public String getUsername(){
		return username;
	}
	public File getFile() {
		return attached_file;
	}

	public boolean hasFileAttached() {
		return !filepath.equals("");
	}

	public Topic getKey() {
		return key;
	}

	public void setKey(Topic key) {
		this.key = key;
	}

	public Value getValue() {
		return value;
	}

	public void setValue(Value value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return "Data{" +
				"key=" + key +
				", value=" + value +
				'}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Data data = (Data) o;
		return Objects.equals(key, data.key) &&
				Objects.equals(value, data.value);
	}

	@Override
	public int hashCode() {
		return Objects.hash(key, value);
	}
}
