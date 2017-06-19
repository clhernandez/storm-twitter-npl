package cl.usach.storm.npl.eda;

public class Tweet {
	private String user;
	private String text;
	private String created_at;

	public Tweet(String user, String text, String created_at) {
		super();
		this.user = user;
		this.text = text;
		this.created_at = created_at;
	}
	public String getUser() {
		return user;
	}
	public void setUser(String user) {
		this.user = user;
	}
	public String getText() {
		return text;
	}
	public void setText(String text) {
		this.text = text;
	}
	public String getCreated_at() {
		return created_at;
	}
	public void setCreated_at(String created_at) {
		this.created_at = created_at;
	}
	@Override
	public String toString() {
		return "Tweet [user=" + user + ", text=" + text + ", created_at=" + created_at + "]";
	}
}
