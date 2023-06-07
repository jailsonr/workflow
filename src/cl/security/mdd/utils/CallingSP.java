package cl.security.mdd.utils;

import org.apache.log4j.Logger;

public class CallingSP {

	private static final LoadProperties load = new LoadProperties();

	private static final Logger log = null;
	
	public static final String SPCALL = "/kondor/configuracion/Workflow/config/SP.properties"; 
	
	public static final String KGRGET = load.getProperties(SPCALL, log).get("KGRGet").toString().trim();
	
	public static final String FLAGS = load.getProperties(SPCALL, log).get("Flags").toString().trim();
	
	public static final String MLSGET = load.getProperties(SPCALL, log).get("MLSGET").toString().trim();
	
	public static final String EDAI = load.getProperties(SPCALL, log).get("ExceededAcceptance").toString().trim();
	
	public static final String EDI = load.getProperties(SPCALL, log).get("ExceededDeals").toString().trim();
	
	public static final String MLSRESULT = load.getProperties(SPCALL, log).get("MLSResult").toString().trim();
	
	public static final String KISFIELDS = load.getProperties(SPCALL, log).get("KISFIELDS").toString().trim();
	
	public static final String GETKISDEAL = load.getProperties(SPCALL, log).get("GETKISDEAL").toString().trim();
	
	public static final String  DEAL = load.getProperties(SPCALL, log).get("DealList").toString().trim();
	
	public static final String  MESSAGES = load.getProperties(SPCALL, log).get("MESSAGES").toString().trim();
	
	
}
