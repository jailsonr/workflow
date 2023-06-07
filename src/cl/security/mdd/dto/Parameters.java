package cl.security.mdd.dto;

public class Parameters {

	private String puerto;
	
	public Parameters() {
		
	}
	
	public Parameters(String puerto) {
		super();
		this.puerto = puerto;
	}

	public String getPuerto() {
		return puerto;
	}

	public void setPuerto(String puerto) {
		this.puerto = puerto;
	}

	@Override
	public String toString() {
		return "Parameters [puerto=" + puerto + "]";
	}
	
}
