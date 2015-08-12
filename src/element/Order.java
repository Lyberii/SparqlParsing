package element;

public enum Order {
	ASC(1),DESC(-1);
	
	private String name;
	private int index;

	private Order(int index) {
		this.index=index;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getIndex() {
		return index;
	}

	public void setIndex(int index) {
		this.index = index;
	}
	
	
}
