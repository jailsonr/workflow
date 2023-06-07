package cl.security.mdd.dao;

import java.sql.CallableStatement;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import cl.security.mdd.utils.CallingSP;
import cl.security.mdd.utils.ConnectionKondor;

public class CustomWindowsUpdater {

	 
	
	public boolean queryUpdateRepairKGR(ConnectionKondor connKondor, int dealId, int kdbTablesId,
			String repKGR, String repMLS,String envBO, Logger log) {
		CallableStatement cs = null;
		
		System.out.println("Ejecutando "+CallingSP.FLAGS+" DealId: "+dealId);
		log.info("Ejecutando "+CallingSP.FLAGS+" DealId: "+dealId);
		String storeProcedure = "{call Kustom.."+CallingSP.FLAGS+"(?,?,?,?,?,?)}";
		log.info("Ejecutando sp: "+storeProcedure);
		
		try {
			cs = connKondor.getConnection().prepareCall(storeProcedure);
		}catch(SQLException e) {
			log.error("Error al crear llamada al SP WKF_updateFlagsDeals");
            log.error("Razon: " + e.getMessage());
		}
		
		try {
			 cs.setString(1, "U");
	            cs.setInt(2, kdbTablesId);
	            cs.setInt(3, dealId);
	            cs.setString(4, repKGR);
	            cs.setString(5, repMLS);
	            cs.setString(6, envBO);
		}catch(SQLException e) {
			log.error("Error al setear parametros a SP WKF_updateFlagsDeals");
            log.error("Razon: " + e.getMessage());
		} try {
            cs.execute();
        }
		catch(SQLException e) {
			log.error("Error al crear llamada al SP WKF_updateFlagsDeals");
            log.error("Razon: " + e.getMessage());
		}
		return true;
	}
}
