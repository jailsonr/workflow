package cl.security.mdd.utils;

import org.apache.log4j.Logger;


public class ConnectionServices {

	@SuppressWarnings("static-access")
	public static ConnectionKondor ConnectionKondor(String url, final Logger log) {
	        ConnectionKondor servicio = new ConnectionKondor();
	        servicio.openConnection(url, log);
	        System.out.println(url);
	        return servicio;
	    }
	
	 @SuppressWarnings("static-access")
	public static ConnectionKGR ConnectionKGR(String url2, final Logger log) {
	        ConnectionKGR servicio = new ConnectionKGR();
	        servicio.openConnection(url2, log);
	        System.out.println(url2);
	        return servicio;
	    }
	
}
