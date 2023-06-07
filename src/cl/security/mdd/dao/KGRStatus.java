package cl.security.mdd.dao;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.lang.management.ManagementFactory;
import java.sql.SQLException;

import cl.security.mdd.utils.CallingSP;
import cl.security.mdd.utils.ConnectionKGR;
import cl.security.mdd.utils.ConnectionKondor;
import cl.security.mdd.utils.Constants;

public class KGRStatus {

	private static CallingSP sp = new CallingSP();
	Constants c = new Constants();

	public int status(ConnectionKGR connKGR, ConnectionKondor connKondor, Logger log, int kdbTablesId, int dealId,
			int transactionId, String action, int version, int retries) {
		
		PropertyConfigurator.configure(Constants.LOG4J);
		log = Logger.getLogger(KGRStatus.class);
		
		int KGRStatus = 0;
		String storeProcedure = "{call " + sp.KGRGET + "(?,?,?,?,?,?,?,?)}";
		
		CallableStatement cs = null;

		try {
			connKGR.openConnection(c.URL2, log);
			cs = connKGR.getConnection().prepareCall(storeProcedure);
			
			System.out.println();
		} catch (SQLException ex) {
			log.info("No se pudo ejecutar el procedimiento WKF_KGRStatus_get");
			log.info("Motivo: " + ex);
		}
		
		try {
            cs.setInt(1, kdbTablesId);
        } catch (SQLException e) {
        	 log.error("No se pudo setear el valor del KdbTableId");
        }
		try {
            cs.setInt(2, dealId);
        }
        catch (SQLException e) {
        	 log.error("No se pudo setear el valor del dealId");
            log.error("Motivo: " + e);
        }
		try {
            cs.setInt(3, transactionId);
        }
        catch (SQLException e) {
            log.error("No se pudo setear el valor del TransactionId");
            log.error("Motivo: " + e);
        }
		try {
            cs.setString(4, action);
        }
        catch (SQLException e) {
            log.error("No se pudo setear el valor del Action");
            log.error("Motivo : " + e);
        }
        try {
            cs.setInt(5, version);
        }
        catch (SQLException e) {
            log.error("No se pudo setear el valor del Version");
            log.error("Motivo: " + e);
        }
        try {
            cs.setInt(6, retries);
        }
        catch (SQLException e) {
            log.error("No se pudo setear el valor del Retries");
            log.error("Motivo: " + e);
        }
        try {
            cs.registerOutParameter(7, 4);
        }
        catch (SQLException e) {
            log.error("No se pudo obtener el valor del Salida Output");
            log.error(("Raz\u00f3n: " + e.getMessage()));
        }
        try {
            cs.registerOutParameter(8, 12);
        }
        catch (SQLException e) {
            log.error("No se pudo obtener el valor del Salida Output");
            log.error(("Raz\u00f3n: " + e.getMessage()));
        }
        try {
            cs.execute();
        }
        catch (SQLException e) {
            log.error("No se pudo ejecutar el SP");
            log.error(("Raz\u00f3n: " + e.getMessage()));
        }
        try {
            KGRStatus = cs.getInt(7);
            String status = cs.getString(8);
           
            log.info(("[ProcessId " + ManagementFactory.getRuntimeMXBean().getName() + "] "
            		+ "Info Processed Deal [TransactionId, KdbTableId, DealId, "
            		+ "Action, Retries, Version, KGRStatus, KGRRequest]: [" 
            		+ transactionId + "," + kdbTablesId + "," 
            		+ dealId + "," + action + "," + retries + "," 
            		+ version + "," + KGRStatus + "," + status + "]"));
        }
        catch (SQLException e2) {
            log.error("No se pudo obtener el valor KGRStatus");
            log.error("Raz\u00f3n: " + e2.getMessage());
        }
      
        return KGRStatus;
		
	}

	public String statusFromCustomWindow(ConnectionKondor connKondor, Logger log, int kdbTableId, int dealId) {
		CallableStatement cs = null;
		ResultSet rs = null;
		final String spCall = "{call " + sp.FLAGS + "(?,?,?,?,?,?)}";
		log.info("Ejecutando WKF_updateFlagsDeals con los parametros : [S," + kdbTableId + "," + dealId
				+ ",null,null,null]");
		
		try {
			connKondor.openConnection(c.URL, log);
			cs = connKondor.getConnection().prepareCall(spCall);
		} catch (SQLException e) {
			log.error("Error al crear llamada al SP WKF_updateFlagsDeals");
			log.error(("Razon: " + e.getMessage()));
		}
		try {
			cs.setString(1, "S");
			cs.setInt(2, kdbTableId);
			cs.setInt(3, dealId);
			cs.setString(4, null);
			cs.setString(5, null);
			cs.setString(6, null);
		} catch (SQLException e) {
			log.error("Error al setear parametros a WKF_updateFlagsDeals");
			log.error("Razon: " + e.getMessage());
		}
		try {
			rs = cs.executeQuery();
		} catch (SQLException e) {
			log.error("Error al ejecutar SP WKF_updateFlagsDeals");
			log.error(("Razon: " + e.getMessage()));
		}
		try {
			if (rs.next()) {
				if (rs.getObject("RepKGR") != null) {
					return rs.getString("RepKGR");
				}
				return "";
			}
		} catch (SQLException e) {
			log.error("Error al obtener los resultados del SP WKF_updateFlagsDeals");
			log.error("Razon: " + e.getMessage());
		}
		return "";
	}

	public void acceptanceLogger(final ConnectionKondor connKondor, final String application, final int kdbTablesId,
			final int dealsId, final Logger log) {
		final String spCall = "{call Kustom.." + sp.EDAI + "(?,?,?)}";
		log.info("Ejecutando WKF_ExceededDeals_acceptanceInsert  TEST");
		CallableStatement callableStatement = null;
		try {
			connKondor.openConnection(c.URL, log);
			callableStatement = connKondor.getConnection().prepareCall(spCall);
		} catch (SQLException e) {
			log.error("Error al crear llamada al SP WKF_ExceededDeals_acceptanceInsert");
			log.error("Razon: " + e.getMessage());
		}
		try {
			callableStatement.setString(1, application);
			callableStatement.setInt(2, kdbTablesId);
			callableStatement.setInt(3, dealsId);
		} catch (SQLException e) {
			log.error("Error al setear parametros a WKF_ExceededDeals_acceptanceInsert");
			log.error("Razon: " + e.getMessage());
		}
		try {
			callableStatement.execute();
		} catch (SQLException e) {
			log.error("Error al ejecutar SP WKF_ExceededDeals_acceptanceInsert");
			log.error("Razon: " + e.getMessage());
		}
		
	}

	public void overdraftLogger(final ConnectionKondor connKondor, final String application, final int transactionId,
			final String action, final int kdbTablesId, final int dealsId, final Logger log) {
		
		CallableStatement cs = null;
		final String storeProcedure = "{call Kustom.." + sp.EDI + "(?,?,?,?,?)}";
		log.info("Ejecutando:  "+sp.EDI+" - KGRStatus");
		
		try {
			connKondor.openConnection(Constants.URL, log);
			cs = connKondor.getConnection().prepareCall(storeProcedure);
		} catch (SQLException e) {
			log.error("Error al crear llamada al SP WKF_ExceededDeals_insert");
			log.error("Razon: " + e.getMessage());
		}
		try {
			cs.setString(1, application);
			cs.setInt(2, transactionId);
			cs.setString(3, action);
			cs.setInt(4, kdbTablesId);
			cs.setInt(5, dealsId);
			
			log.info("application: " + application + " - transactionId: " + transactionId);
			log.info("action: " + action + " - kdbTablesId: " + kdbTablesId + " - dealsId: " + dealsId);
			System.out.println("Parametros: "+cs.toString());
			
		} catch (SQLException e) {
			log.error("Error al setear parametros a WKF_ExceededDeals_insert");
			log.error("Razon: " + e.getMessage());
		}
		try {
			cs.execute();
		} catch (SQLException e) {
			log.error("Error al ejecutar SP WKF_ExceededDeals_insert");
			log.error("Razon: " + e.getMessage());
		}
		
	}

}
