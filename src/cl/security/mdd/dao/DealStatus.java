package cl.security.mdd.dao;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Stack;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import cl.security.mdd.Main;
import cl.security.mdd.dto.Deal;
import cl.security.mdd.dto.Messages;
import cl.security.mdd.utils.CallingSP;
import cl.security.mdd.utils.ConnectionKGR;
import cl.security.mdd.utils.ConnectionKondor;
import cl.security.mdd.utils.Constants;
import cl.security.mdd.utils.ExecuteKondorServices;

public class DealStatus extends Thread {

	private int kdbTableId;
	private int dealId;
	private int retries;
	private int staticRetries;
	private int version;
	private int kgrStatusValue;
	private int mlsStatusValue;
	private int transactionId;

	private String action;
	private Deal deal;
	private Stack stack;
	private ConnectionKondor conKondor;
	private ConnectionKGR conKGR;
	private static Logger log = null;
	private boolean isStatusValueProcess = true;

	private static String cwTableName;
	private String repaMLS;
	private Constants cons = new Constants();
	private static CallingSP sp = new CallingSP();

	public DealStatus(Deal deal, Logger log, ConnectionKondor conKondor, ConnectionKGR conKGR, Stack stack) {

		this.kdbTableId = deal.getKdbTableId();
		this.dealId = deal.getDealId();
		this.transactionId = deal.getTransactionId();
		this.action = deal.getAction();
		this.log = log;
		this.conKondor = conKondor;
		this.conKGR = conKGR;
		this.deal = deal;
		this.stack = stack;
		this.retries = deal.getRetries();
		this.version = deal.getVersion();
		this.isStatusValueProcess = true;
	}

	public DealStatus() {}

	private void faseUno() throws SQLException {
		ExecuteKondorServices exec = new ExecuteKondorServices();
		KGRStatus getKGR = new KGRStatus();
		MLSStatus getMLS = new MLSStatus();
		PropertyConfigurator.configure(Constants.LOG4J);
		log = Logger.getLogger(DealStatus.class);

		this.kgrStatusValue = getKGR.status(conKGR, conKondor, log, kdbTableId, dealId,
				transactionId, action, version, retries);

		this.mlsStatusValue = getMLS.status(conKondor, log, kdbTableId, dealId);

		do {
			switch (kgrStatusValue) {
			case 0: {
				staticRetries = Integer.parseInt(cons.RETRIES);
				if (deal.getRetries() != staticRetries) {
					try {
						log.info("Thread ID :" + Thread.currentThread().getId()
								+ " - Se esta pausando la ejecución por: " + deal.getRetries() * 5 + " segundos.");
						Thread.currentThread();
						Thread.sleep(deal.getRetries() * 5000);
					} catch (InterruptedException e) {
						log.error("No se puede pausar la ejecución del Hilo");
						log.error("Motivo: " + e);
						this.isStatusValueProcess = false;
					}

					this.deal.setRetries(deal.getRetries() + 1);
					retries = deal.getRetries();
					kgrStatusValue = getKGR.status(conKGR, conKondor, log, kdbTableId, dealId, transactionId, action,
							version, retries);
					this.isStatusValueProcess = true;
					//continue;
					break;
				}

				removeStack(deal.getDealId(), MessagesDAO.stack);
				log.info("[ThreadId " + Thread.currentThread().getId() + "] isStatusValueProcess: "
						+ this.isStatusValueProcess);
				this.isStatusValueProcess = false;
				//continue;
				break;
			}

			case 1: {
				staticRetries = Integer.parseInt(cons.RETRIES);
				if (deal.getRetries() != staticRetries) {
					try {
						log.info("Thread ID :" + Thread.currentThread().getId()
								+ " - Se esta pausando la ejecución por: " + deal.getRetries() * 5 + " segundos.");
						Thread.currentThread();
						Thread.sleep(deal.getRetries() * 5000);
					} catch (InterruptedException e) {
						log.error("No se puede pausar la ejecución del Hilo");
						log.error("Motivo: " + e);
					}
					this.deal.setRetries(deal.getRetries() + 1);
					this.retries = this.deal.getRetries();
					this.kgrStatusValue = getKGR.status(conKGR, conKondor, log, kdbTableId, dealId, transactionId,
							action, version, retries);
					this.isStatusValueProcess = true;
					//continue;
					break;
				}
				removeStack(deal.getDealId(), MessagesDAO.stack);
				log.info("[ThreadId " + Thread.currentThread().getId() + "] isStatusValueProcess: "
						+ this.isStatusValueProcess);
				this.isStatusValueProcess = false;
				//continue;
				break;
			} // case 1

			/*default: {
				continue;
			}*/

			case 2: {
				if (this.mlsStatusValue == 0) {
					String filename = null;
					KisFileDAO create = new KisFileDAO();
					queryUpdateRepairKGR(conKondor, dealId, kdbTableId, "N", "N", "N");
					filename = create.importFile(dealId, kdbTableId, kgrStatusValue, "Y", conKondor, log);
					log.info("Archivo KplusImport creado: [" + filename + "]");
					removeStack(deal.getDealId(), MessagesDAO.stack);
					queryUpdateWKFDealsList(conKondor, dealId, kdbTableId, transactionId);
				} else {
					queryUpdateRepairKGR(conKondor, dealId, kdbTableId, "N","R","S");
					removeStack(deal.getDealId(), MessagesDAO.stack);
					queryUpdateWKFDealsList(conKondor, deal.getDealId(), deal.getKdbTableId(), deal.getTransactionId());
				}

				this.isStatusValueProcess = false;
				//continue;
				break;
			}

			case 3: {
				if (this.mlsStatusValue == 0) {
					this.repaMLS = "N";
				} else {
					this.repaMLS = "R";
				}

				queryUpdateRepairKGR(conKondor, dealId, kdbTableId, "R", repaMLS, "S");
				removeStack(deal.getDealId(), MessagesDAO.stack);
				queryUpdateWKFDealsList(conKondor, deal.getDealId(), deal.getKdbTableId(), deal.getTransactionId());
				this.isStatusValueProcess = false;
				//continue;
				break;
			}

			case 4: {
				if (this.mlsStatusValue == 0) {
					this.repaMLS = "N";
				} else {
					this.repaMLS = "R";
				}

				queryUpdateRepairKGR(conKondor, dealId, kdbTableId, "R", repaMLS, "N");
				removeStack(deal.getDealId(), MessagesDAO.stack);
				queryUpdateWKFDealsList(conKondor, deal.getDealId(), deal.getKdbTableId(), deal.getTransactionId());
				this.isStatusValueProcess = false;
				//continue;
				break;
			}

			}// fin switch

		} while (isStatusValueProcess);

		if (mlsStatusValue == 1) {
			getMLS.overDraftLogger(conKondor ,"MLS", deal.getTransactionId() , deal.getAction(), deal.getKdbTableId(), deal.getDealId(), log);
		}
		if (kgrStatusValue == 3 || kgrStatusValue == 4 ) {
			getKGR.overdraftLogger(conKondor ,"KGR", deal.getTransactionId() , deal.getAction(), deal.getKdbTableId(), deal.getDealId(), log);
		}
		
		

	} // fin metodo

	private void faseDos() throws SQLException {
		ExecuteKondorServices exec = new ExecuteKondorServices();
		KGRStatus getKGR = new KGRStatus();
		boolean excessKGR = false;
		boolean excessMLS = false;

		kgrStatusValue = getKGR.status(conKGR, conKondor, log, kdbTableId, dealId, transactionId, action, version,
				retries);
		log.info("[ThreadId " + Thread.currentThread().getId() + "] Starting KGR Excess Checking on " + dealId);

		do {
			switch (kgrStatusValue) {
			case 0: {
				staticRetries = Integer.parseInt(cons.RETRIES);
				if (deal.getRetries() != staticRetries) {
					try {
						System.out.println("[ThreadId " + Thread.currentThread().getId()
								+ "] Se est\u00e1 pausando la ejecuci\u00f3n por " + deal.getRetries() * 5
								+ " segundos");
						// log.info("[ThreadId " + Thread.currentThread().getId() + "] Se est\u00e1
						// pausando la ejecuci\u00f3n por " + deal.getRetries() * 5 + " segundos");
						Thread.currentThread();
						Thread.sleep(deal.getRetries() * 5000);
					} catch (InterruptedException e) {
						log.error("No se puedo pausar ejecuci\u00f3n del Thread");
						log.error("Raz\u00f3n:" + e.getMessage());
						this.isStatusValueProcess = false;
					}
					deal.setRetries(deal.getRetries() + 1);
					retries = deal.getRetries();
					this.kgrStatusValue = getKGR.status(conKGR, conKondor, log, kdbTableId, dealId, transactionId,
							action, version, retries);
					this.isStatusValueProcess = true;
					//continue;
					break;
				}
				removeStack(deal.getDealId(), MessagesDAO.stack);
				System.out.println("[ThreadId " + Thread.currentThread().getId() + "] isStatusValueProcess: "
						+ this.isStatusValueProcess);
				
				this.isStatusValueProcess = false;
				//continue;
				break;
			}
			case 1: {
				staticRetries = Integer.parseInt(cons.RETRIES);
				if (deal.getRetries() != this.staticRetries) {
					try {
						System.out.println("[ThreadId " + Thread.currentThread().getId()
								+ "] Se est\u00e1 pausando la ejecuci\u00f3n por " + deal.getRetries() * 5
								+ " segundos");
						// log.info("[ThreadId " + Thread.currentThread().getId() + "] Se est\u00e1
						// pausando la ejecuci\u00f3n por " + deal.getRetries() * 5 + " segundos");
						Thread.currentThread();
						Thread.sleep(deal.getRetries() * 5000);
					} catch (InterruptedException e) {
						log.error("No se puedo pausar ejecuci\u00f3n del Thread");
						log.error("Raz\u00f3n:" + e.getMessage());
					}
					deal.setRetries(deal.getRetries() + 1);
					retries = deal.getRetries();
					this.kgrStatusValue = getKGR.status(conKGR, conKondor, log, kdbTableId, dealId, transactionId,
							action, version, retries);
					this.isStatusValueProcess = true;
					//continue;
					break;
				}
				removeStack(deal.getDealId(), MessagesDAO.stack);
				System.out.println("[ThreadId " + Thread.currentThread().getId() + "] isStatusValueProcess: "
						+ this.isStatusValueProcess);
				// log.info("[ThreadId " + Thread.currentThread().getId() + "]
				// isStatusValueProcess: " + this.isStatusValueProcess);
				this.isStatusValueProcess = false;
				//continue;
				break;
			}
			/*default: {
				continue;
			}*/
			case 2: {
				excessKGR = false;
				this.isStatusValueProcess = false;
				//continue;
				break;
			}
			case 3: {
				excessKGR = true;
				this.isStatusValueProcess = false;
				//continue;
				break;
			}
			case 4: {
				excessKGR = true;
				this.isStatusValueProcess = false;
				//continue;
				break;
			}
			}
		} while (isStatusValueProcess);
		System.out.println(
				"[ThreadId " + Thread.currentThread().getId() + "] Starting MLS Excess Checking on " + this.dealId);
		
		MLSStatus getMLS = new MLSStatus();
		boolean isMLSValDone = false;
		this.staticRetries = Integer.parseInt(cons.RETRIES);
		deal.setRetries(0);
		do {
			isMLSValDone = getMLS.statusReady(conKondor, log, kdbTableId, dealId, transactionId);
			if (isMLSValDone) {
				break;
			}
			if (deal.getRetries() == staticRetries) {
				continue;
			}
			try {
				System.out.println("[ThreadId " + Thread.currentThread().getId()
						+ "] Se est\u00e1 pausando la ejecuci\u00f3n por " + deal.getRetries() * 5 + " segundos");
				
				Thread.currentThread();
				Thread.sleep(deal.getRetries() * 5000);
			} catch (InterruptedException e2) {
				log.error("No se puedo pausar ejecuci\u00f3n del Thread");
				log.error("Motivo:" + e2.getMessage());
			}
			deal.setRetries(deal.getRetries() + 1);
			retries = deal.getRetries();
		} while (!isMLSValDone && retries < staticRetries);
		if (this.retries == this.staticRetries) {
			System.out.println(
					"[ThreadId " + Thread.currentThread().getId() + "] Maximum number of retries reached for DealId "
							+ this.deal.getDealId() + ". It will be removed from the stack and processed on next call");
			
			
			removeStack(deal.getDealId(), MessagesDAO.stack);
		} else {
			if (getMLS.status(conKondor, log, kdbTableId, dealId) == 1) {
				excessMLS = true;
			}
			queryUpdateRepairKGR(conKondor, dealId, kdbTableId, excessKGR ? "R" : "N",
					excessMLS ? "R" : "N", (!excessKGR && !excessMLS) ? "N" : "S");
			removeStack(deal.getDealId(), MessagesDAO.stack);
			queryUpdateWKFDealsList(conKondor, deal.getDealId(), deal.getKdbTableId(), deal.getTransactionId());
			
			
			
			if (!excessKGR && !excessMLS) {
				String FileName = null;
				final KisFileDAO create = new KisFileDAO();
				FileName = create.importFile(dealId, kdbTableId, kgrStatusValue, "Y", conKondor,
						log);
				log.info("Archivo KplusImport creado: [" + FileName + "]");
			}
			if (excessMLS) {
				getMLS.overDraftLogger(conKondor, "MLS", deal.getTransactionId(), deal.getAction(),
						deal.getKdbTableId(), deal.getDealId(), log);
			}
			if (excessKGR) {
				getKGR.overdraftLogger(conKondor, "KGR", deal.getTransactionId(), deal.getAction(),
						deal.getKdbTableId(), deal.getDealId(), log);
			}
		}
	}

	
	@Override
	public void run() {
		if (this.kdbTableId == 119 || this.kdbTableId == 8) {
			try {
				faseDos();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		else {
			try {
				faseUno();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		//TEST FRAN
		log.info("Cierro conexion DealStatus");
		conKondor.closeConnection(log);
		conKGR.closeConnection(log);
		
	}

	public static void removeStack(final int DealId, final Stack<Deal> stack) {
		final Iterator it = stack.iterator();
		for (int i = 0; i < stack.size(); ++i) {
			 Deal stackValue = (Deal) stack.get(i);
			 int dealIdStack = stackValue.getDealId();
			 String Status = stackValue.getDealStatus();
			if (dealIdStack == DealId) {
				log.info("[ThreadId " + Thread.currentThread().getId() + "] Numero de elementos en la Pila: "
						+ stack.size());
				stack.remove(i);
				log.info("[ThreadId " + Thread.currentThread().getId() + "] El Deal " + DealId
						+ " se proceso con estado " + Status + " y se elimino de la Pila");
				log.info("[ThreadId " + Thread.currentThread().getId() + "] Numero de elementos en la Pila: "
						+ stack.size());
				break;
			}
		}
	}

	public static boolean queryUpdateRepairKGR(ConnectionKondor connKondor, int dealId, int kdbTableId, String repKGR,
			String repMLS, String envBO) {
		Constants c = new Constants();
		CallableStatement cs = null;
		final String spCall = "{call Kustom.." + sp.FLAGS + "(?,?,?,?,?,?)}";
		log.info("Ejecutando WKF_updateFlagsDeals DealId: "+dealId);
		
		try {
			
			connKondor.openConnection(c.URL2, log);
			cs = connKondor.getConnection().prepareCall(spCall);
		} catch (SQLException e) {
			log.error("Error al crear llamada al SP WKF_updateFlagsDeals");
			log.error("Razon: " + e.getMessage());
		}
		try {
			cs.setString(1, "U");
			cs.setInt(2, kdbTableId);
			cs.setInt(3, dealId);
			cs.setString(4, repKGR);
			cs.setString(5, repMLS);
			cs.setString(6, envBO);
		} catch (SQLException e) {
			log.error("Error al setear parametros a WKF_updateFlagsDeals");
			log.error("Razon: " + e.getMessage());
		}
		try {
			cs.execute();
		} catch (SQLException e) {
			log.error("Error al ejecutar SP WKF_updateFlagsDeals");
			log.error("Razon: " + e.getMessage());
		}
		
		
		return true;
	}

	public static boolean queryUpdateWKFDealsList(ConnectionKondor connKondor, int dealId,
			int kdbTableId, int transactionId) throws SQLException  {
		Statement stmt = null;
		
		String queryUpdateRepair = "UPDATE " + CallingSP.DEAL + " SET Status = 'T' WHERE DealId = " + dealId
				+ " AND KdbTableId = " + kdbTableId + " AND TransactionId = " + transactionId;
		
		log.info("[ThreadId " + Thread.currentThread().getId() + "] QueryUpdateRepair: " + queryUpdateRepair);
		

		try {
			stmt = connKondor.getConnection().createStatement();
			
		} catch (SQLException e) {
			log.error("Error al crear el Statement");
			log.error("Raz\u00f3n : " + e.getMessage());
		}
		try {
			stmt.executeUpdate(queryUpdateRepair);
			
		} catch (SQLException e) {
			log.error("Error al crear el ResultSet");
			log.error("Raz\u00f3n : " + e.getMessage());
		}
		eliminar();
		return true;
	}
	
	
	public static void eliminar() throws SQLException {

		ConnectionKondor con = new ConnectionKondor();
		con.openConnection(Constants.URL, log);
		
		Statement stmt = null;
		ResultSet rs = null;
		ResultSet rs2 = null;
		PreparedStatement ps = null;
		
		
		String query = "SELECT * FROM Kustom..WKF_MessagesInProgress";
		
		try {
			stmt = con.getConnection().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
	                ResultSet.CONCUR_READ_ONLY, 
	                ResultSet.HOLD_CURSORS_OVER_COMMIT);
			rs = stmt.executeQuery(query);
			
			while(rs.next()) {
				String params = rs.getString(3);
				
				String []nparams = params.split("\\s+");
				String a = nparams[0]; // Kondor o KGR
				int kdbtable = Integer.parseInt(nparams[1]); //kdbTables
				int dealIdMP= Integer.parseInt(nparams[2]);//DealId
				
				
				try {
					String query2 = "SELECT * FROM Kustom..WKF_DealsList dl, Kustom..WKF_MessagesInProgress "
							+ "WHERE dl.DealId = "+dealIdMP+" AND dl.Status = 'T'";
					 Statement st2 = con.getConnection().createStatement(
		                        ResultSet.TYPE_SCROLL_INSENSITIVE,
		                        ResultSet.CONCUR_READ_ONLY);
					
					rs2 = st2.executeQuery(query2);
					
					
					int dealDL =0;
					char status=0;
					while(rs2.next()) {
						
						dealDL = rs2.getInt("DealId");
						status = rs2.getString("Status").charAt(0);
						//deal de messages - deal de dealList
						
						if(dealDL == dealIdMP && status=='T') {
							System.out.println("Se eliminara el siguiente mensaje:  "+nparams[0]+" - "+nparams[1]+" - "+nparams[2]);
							ps = con.getConnection().prepareStatement("Delete from Kustom..WKF_MessagesInProgress "
									+ "where InterfaceParams like '%"+dealIdMP+"%'");
							ps.executeUpdate();
						}else {
							System.out.println("No es posible eliminar el mensaje");
						}
					}
					log.info("Se eliminara el siguiente mensaje:  "+nparams[0]+" - "+nparams[1]+" - "+nparams[2]);
					System.out.println(dealDL+" - "+status);
					
				}catch(SQLException e) {
					System.out.println("error 1: "+e);
				}
			}
		}catch(SQLException e) {
			System.out.println("error 2: "+e);
		}finally {
			rs.close();
			stmt.close();
			ps.close(); 
			rs2.close();
			con.closeConnection(log);	
		}
	
	}

	

}
