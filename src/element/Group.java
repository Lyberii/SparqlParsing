package element;

import java.util.List;

public class Group {
	private List<String> names;
	private List<Extend> aggregates;
	
	public List<String> getNames() {
		return names;
	}
	public void setNames(List<String> names) {
		this.names = names;
	}
	public List<Extend> getAggregates() {
		return aggregates;
	}
	public void setAggregates(List<Extend> aggregates) {
		this.aggregates = aggregates;
	}
}
