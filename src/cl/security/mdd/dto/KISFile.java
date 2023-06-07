package cl.security.mdd.dto;

public class KISFile {

	private int paramId;
	private String paramName;
	private String paramValue;
	

	public int getParamId() {
		return paramId;
	}

	public void setParamId(int paramId) {
		this.paramId = paramId;
	}

	public String getParamName() {
		return paramName;
	}

	public void setParamName(String paramName) {
		this.paramName = paramName;
	}

	public String getParamValue() {
		return paramValue;
	}

	public void setParamValue(String paramValue) {
		this.paramValue = paramValue;
	}

	@Override
	public String toString() {
		return "KISFile [paramId=" + paramId + ", paramName=" + paramName + ", paramValue=" + paramValue + "]";
	}
	
	
}
