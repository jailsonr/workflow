package cl.security.mdd.dao;

import java.lang.management.ManagementFactory;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.Types;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import cl.security.mdd.utils.CallingSP;
import cl.security.mdd.utils.ConnectionKondor;
import cl.security.mdd.utils.Constants;

public class MLSStatus {

	private static CallingSP sp = new CallingSP();

	public static final int PROCESS_PENDING = 0;
	public static final int PROCESSED = 1;

	public int status(ConnectionKondor connKondor, Logger log, int kdbTablesId, int dealId) {
		int MLSStatus = 0;

		PropertyConfigurator.configure(Constants.LOG4J);
		log = Logger.getLogger(MLSStatus.class);

		String storeProcedure = "{call Kustom.." + sp.MLSGET + "(?,?,?)}";
		CallableStatement cs = null;

		connKondor.openConnection(Constants.URL, log);

		try {
			cs = connKondor.getConnection().prepareCall(storeProcedure);
			

		} catch (SQLException ex) {
			log.error("No se pudo ejecutar el procedimiento WKF_MLSStatus_get");
			log.error("Motivo: " + ex);
		}

		try {
			cs.setInt(1, kdbTablesId);
		} catch (SQLException e) {
			log.error("No se pudo setear el valor del KdbTableId");
			log.error("Motivo: " + e);
		}
		try {
			cs.setInt(2, dealId);
		} catch (SQLException e) {
			log.error("No se pudo setear el valor del DealId");
			log.error("Motivo: " + e);
		}

		try {
			cs.registerOutParameter(3, 4);
		} catch (SQLException e) {
			log.error("No se puede obtener el parametro de salida Output");
			log.error("Motivo: " + e);
		}
		try {
			cs.execute();
		} catch (SQLException e) {
			log.error("No se pudo ejecutar el SP WKF_MLSStatus_get");
			log.error("Motivo: " + e);
		}

		try {
			MLSStatus = cs.getInt(3);
			System.out.println("[ProcessID: " + ManagementFactory.getRuntimeMXBean().getName()
					+ "] - Información procesada [KdbTables_Id,DealId,ResultMls]: [" + kdbTablesId + " - " + dealId
					+ " - " + MLSStatus + "]");
			log.info("MLSSTATUS - STATUS ----[ProcessID: " + ManagementFactory.getRuntimeMXBean().getName()
					+ "] - Información procesada [KdbTables_Id,DealId,ResultMls]: [" + kdbTablesId + " - " + dealId
					+ " - " + MLSStatus + "]");
		} catch (SQLException e) {
			log.error("No se pudo obtener el valor MLSStatus");
			log.error("Motivo: " + e);
		}

		return MLSStatus;
	}

	public String statusFromCustomWindow(ConnectionKondor connKondor, Logger log, int kdbTablesId, int dealId) {
		CallableStatement cs = null;
		ResultSet rs = null;

		String storeProcedure = "{call " + sp.FLAGS + "(?,?,?,?,?,?)}";
		System.out.println("Ejecutando WKF_updateFlagsDeals DealId: "+dealId);
		log.info("Ejecutando WKF_updateFlagsDeals DealId: "+dealId);

		try {
			connKondor.openConnection(Constants.URL, log);
			cs = connKondor.getConnection().prepareCall(storeProcedure);
		} catch (SQLException e) {
			log.error("Error al crear llamada al SP WKF_updateFlagsDeals");
			log.error("Motivo: " + e);
		}
		try {
			cs.setString(1, "S");
			cs.setInt(2, kdbTablesId);
			cs.setInt(3, dealId);
			cs.setString(4, null);
			cs.setString(5, null);
			cs.setString(6, null);
			
			
			
		} catch (SQLException e) {
			log.error("Error al setear parametros a WKF_updateFlagsDeals");
			log.error("Motivo: " + e);
		}

		try {
			rs = cs.executeQuery();
		} catch (SQLException e) {
			log.error("Error al ejecutar SP WKF_updateFlagsDeals");
			log.error("Motivo: " + e);
		}

		try {
			if (rs.next()) {
				if (rs.getObject("RepMLS") != null) {
					return rs.getString("RepMLS");
				}
				return "";
			}
		} catch (SQLException e) {
			log.error("Error al obtener los resultados del SP WKF_updateFlagsDeals");
			log.error("Motivo: " + e);
		}
		
		return "";
	}

	public void acceptanceLogger(ConnectionKondor connKondor, String application, int kdbTablesId, int dealsId,
			Logger log) {
		String storedProcedure = "{call " + sp.EDAI + "(?,?,?)}";
		log.info("Ejecutando WKF_ExceededDeals_acceptanceInsert");
		System.out.println("WKF_ExceededDeals_acceptanceInsert: " + application + " - " + kdbTablesId + " - " + dealsId);

		CallableStatement cs = null;

		try {
			connKondor.openConnection(Constants.URL, log);
			cs = connKondor.getConnection().prepareCall(storedProcedure);
		} catch (SQLException e) {
			log.error("Error al crear la llamada al SP WKF_ExceededDeals_acceptanceInsert");
			log.error("Motivo: " + e);
		}

		try {
			cs.setString(1, application);
			cs.setInt(2, kdbTablesId);
			cs.setInt(3, dealsId);
			
		} catch (SQLException e) {
			log.error("Error al setear los parametros de WKF_ExceededDeals_acceptanceInsert");
			log.error("Motivo: " + e);
		}

		try {
			cs.execute();
		} catch (SQLException e) {
			log.error("Error al ejecutar SP WKF_ExceededDeals_acceptanceInsert");
			log.error("Motivo: " + e);
		}

		System.out.println("Ejecutado correctamente WKF_ExceededDeals_acceptanceInsert");
		log.info("Ejecutado correctamente WKF_ExceededDeals_acceptanceInsert");
	}

	public void overDraftLogger(ConnectionKondor connKondor, String application, int transactionId, String action,
			int kdbTablesId, int dealsId, Logger log) {

		CallableStatement cs = null;
		String storeProcedure = "{call Kustom.." + sp.EDI + "(?,?,?,?,?)}";
		log.info("Ejecutando WKF_ExceededDeals_insert - MLSStatus");

		try {
		connKondor.openConnection(Constants.URL, log);
		cs = connKondor.getConnection().prepareCall(storeProcedure);
	
		}
		catch(SQLException e) {
			log.error("Error al crear llamada al SP WKF_ExceededDeals_insert");
			log.error("Razon: " + e.getMessage());
		}
		try {
			log.info("obteniendo parametros");
			cs.setString(1, application);
			cs.setInt(2, transactionId);
			cs.setString(3, action);
			cs.setInt(4, kdbTablesId);
			cs.setInt(5, dealsId);

			log.info("application: " + application + " - transactionId: " + transactionId);
			log.info("action: " + action + " - kdbTablesId: " + kdbTablesId + " - dealsId: " + dealsId);
			

		} catch (SQLException e) {
			log.error("Error al ejecutar SP WKF_ExceededDeals_insert");
			log.error("Motivo: " + e);
		}

		try {
			cs.execute();
		} catch (SQLException e) {
			log.error("Error al ejecutar SP WKF_ExceededDeals_insert");
			log.error("Motivo: " + e);
		}

	}

	public boolean statusReady(ConnectionKondor connKondor, Logger log, int kdbTablesId, int dealId,
			int transactionId) {
		CallableStatement cs = null;
		int status = 0;
		String storeProcedure = "{call Kustom.." + sp.MLSRESULT + "(?,?,?,?)}";
		
		log.info("Ejecuta: "+storeProcedure);
		
		try {
			connKondor.openConnection(Constants.URL, log);
			cs = connKondor.getConnection().prepareCall(storeProcedure);
			log.info("Ejecutando con los parametros: kdbTablesId: "+kdbTablesId+" - dealId:"+dealId+" - transactionId: "+transactionId+" - status: "+status);
		} catch (SQLException e) {
			log.error("Error al ejecutar SP WKF_MLSDealResult_get");
			log.error("Motivo: " + e);
		}

		try {
			cs.setInt(1, kdbTablesId);
			log.info("kdbTablesId: "+kdbTablesId);
		} catch (SQLException e) {
			log.error((Object)"No se pudo setear el valor del KdbTableId");
			log.error("Motivo: " + e);
		}

		try {
			cs.setInt(2, transactionId);
			log.info("transactionId: "+transactionId);
		} catch (SQLException e) {
			log.error((Object)"No se pudo setear el valor del transactionId");
			log.error("Motivo: " + e);
		}
		try {
			cs.setDouble(3, dealId);
			log.info("dealId: "+dealId);
		} catch (SQLException e) {
			log.error((Object)"No se pudo setear el valor del dealId");
			log.error("Motivo: " + e);
		}
		try {
			//cs.registerOutParameter(4,4);
			cs.registerOutParameter(4, Types.INTEGER);
		} catch (SQLException e) {
			log.error((Object)"No se pudo obtener el valor del Salida Output");
			log.error("Motivo: " + e);
		}
		try {
			cs.execute();
		} catch (SQLException e) {
			log.error("No se pudo ejecutar: "+storeProcedure);
			log.error("Motivo: " + e);
		}

		try {
			status = cs.getInt(4);
			 log.info("ProcessID "+ManagementFactory.getRuntimeMXBean().getName() +"] MLS"
			 		+ "Status ready for reading ? [KdbTables_Id,DealId, TransactionId, ResultMls]:"
			 		+ "[" + kdbTablesId + "," + dealId + "," + transactionId + "," + ((status == 0) ? "NOT YET" : "YES") + "]");
		} catch (SQLException e) {
			log.error("No se pudo obtener el valor WKF_MLSDealResult_get");
			log.error("Motivo: " + e);
		}

		return status != 0;
	}

}
