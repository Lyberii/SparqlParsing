package element;

import java.util.List;
import java.util.Set;

public class Triple {
	private List<String> pos;
	private List<String> posType;
	private Set<String> tables;
	
	
	public List<String> getPos() {
		return pos;
	}
	public void setPos(List<String> pos) {
		this.pos = pos;
	}
	public List<String> getPosType() {
		return posType;
	}
	public void setPosType(List<String> posType) {
		this.posType = posType;
	}
	public Set<String> getTables() {
		return tables;
	}
	public void setTables(Set<String> tables) {
		this.tables = tables;
	}
	
}
