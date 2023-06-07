package cl.security.mdd.dao;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Stack;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import cl.security.mdd.dto.Deal;
import cl.security.mdd.dto.Messages;
import cl.security.mdd.utils.CallingSP;
import cl.security.mdd.utils.ConnectionKGR;
import cl.security.mdd.utils.ConnectionKondor;
import cl.security.mdd.utils.Constants;

public class MessagesDAO {

	private String query;
	private String query2;
	private ResultSet rs = null;
	private ResultSet rs2 = null;
	private Statement stmt = null;
	private static Logger log = null;
	public static Stack stack = new Stack();
	private static CustomWindowsUpdater cwu = new CustomWindowsUpdater();

	private ConnectionKondor connKondor = new ConnectionKondor();
	private ConnectionKGR connKGR = new ConnectionKGR();
	private ConnectionKondor connKondor1 = new ConnectionKondor();

	
	
	private Messages m;
	private static Deal d;

	public void messagesInProgress() {
		PropertyConfigurator.configure(Constants.LOG4J);
		log = Logger.getLogger(MessagesDAO.class);
		query = "SELECT * FROM " + CallingSP.MESSAGES;
		query2 = "SELECT * FROM Kustom..WKF_DealsList WHERE Status = 'P' AND Retries = 0";

		String ip;
		int port;
		String params;

		try {
			connKondor1.openConnection(Constants.URL, log);

			stmt = connKondor.getConnection().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
	                ResultSet.CONCUR_READ_ONLY, 
	                ResultSet.HOLD_CURSORS_OVER_COMMIT);
			rs = stmt.executeQuery(query);
		  
			
			
			while (rs.next()) {
				ip = rs.getString(1);
				port = rs.getInt(2);
				params = rs.getString(3);

				m = new Messages(ip, port, params);
				System.out.println(m);
				String [] nparams = params.split("\\s+");
				
				
				if (nparams[0].trim().equals("Kondor")) {
					try {
						System.out.println("Buscando "+nparams[0]);
						Statement st2 = connKondor.getConnection().createStatement(
		                        ResultSet.TYPE_SCROLL_INSENSITIVE,
		                        ResultSet.CONCUR_READ_ONLY);
						rs2 = st2.executeQuery(query2);
						
						while (rs2.next()) {
							if (!isDealProcessing(rs2.getInt("DealId"), rs2.getInt("TransactionId"), stack)) {
								Deal deal = new Deal();
								deal.setDealId(rs2.getInt("DealId"));
								deal.setKdbTableId(rs2.getInt("KdbTableId"));
								deal.setTransactionId(rs2.getInt("TransactionId"));
								deal.setRetries(rs2.getInt("Retries"));
								deal.setAction(rs2.getString("Action"));
								deal.setStatus(rs2.getString("Status"));
								deal.setVersion(rs2.getInt("Version"));

								stack.push(deal);
								
								DealStatus getStatus = new DealStatus(deal, log, connKondor, connKGR, stack);
								

								getStatus.start();
								System.out.println(deal);
							}
						}
						rs2.close();
						
					} catch (SQLException ex) {
						log.error("Error : No se encontraron Deals a Procesar");
						log.error(("Raz\u00f3n: " + ex.getMessage()));
						System.out.println("No se encontraron Deals a Procesar");
					
					}
				}

				else if (nparams[0].trim().equals("KGR")) {
					System.out.println("Entra a KGR");
					log.info("KGR");
					int kdbTablesId = Integer.parseInt(nparams[1].trim());
					int dealsId = Integer.parseInt(nparams[2].trim());
					MLSStatus getMLS = new MLSStatus();
					String reparoMLS = getMLS.statusFromCustomWindow(connKondor, log, kdbTablesId, dealsId);
					getMLS.acceptanceLogger(connKondor, "KGR", kdbTablesId, dealsId, log);
					
					if(reparoMLS.equals("N")) {
						cwu.queryUpdateRepairKGR(connKondor, dealsId, kdbTablesId, "N", reparoMLS,"N", log);
					//Inicio 
						
						 String fileName = null;
				            KisFileDAO create = new KisFileDAO();
				            dealsId = create.getKISDealId(connKondor, log, kdbTablesId, dealsId);
				            
				            fileName = create.importFile(dealsId, kdbTablesId, 0, "Y", connKondor, log);
				            
				            log.info("Archivo KplusImport creado: [" + fileName + "]");
						
				            eliminar();
					//Fin 
					
					}else if(reparoMLS.equals("R")) {
						cwu.queryUpdateRepairKGR(connKondor, dealsId, kdbTablesId, "N", reparoMLS, "S", log);
					}
					
					else {
						log.info("El estado de reparo MLS aun no se encuentra calculado. Omitiendo la actualizacion de flags. DEAL: "+dealsId);
						System.out.println("El estado de reparo MLS aun no se encuentra calculado. Omitiendo la actualizacion de flags");
					}
					
				}else {
					if(!nparams[0].equals("MLS")) {
						continue;
					}
					
					int kdbTablesId = Integer.parseInt(nparams[1].trim());
					int dealsId = Integer.parseInt(nparams[2].trim());
					KGRStatus getKGR = new KGRStatus();
					
					String reparoKGR = getKGR.statusFromCustomWindow(connKondor, log, kdbTablesId, dealsId);
					
					getKGR.acceptanceLogger(connKondor, "MLS", kdbTablesId, dealsId, log);
					
					if(reparoKGR.equals("N")) {
						cwu.queryUpdateRepairKGR(connKondor, dealsId, kdbTablesId, reparoKGR, "N", "N", log);
						String filename = null;
						KisFileDAO create = new KisFileDAO();
						dealsId = create.getKISDealId(connKondor, log, kdbTablesId, dealsId);
						filename = create.importFile(dealsId, kdbTablesId, 0, "Y", connKondor, log);
						log.info("Archivo KplusImport creado: "+filename);

						eliminar();
				        
						
						
						
					}else if(reparoKGR.equals("R")) {
						cwu.queryUpdateRepairKGR(connKondor, dealsId, kdbTablesId, reparoKGR, "N", "S", log);
					}else {
						log.info("El estado de Reparo KGR aun no se encuentra calculado. Omitiendo la actualizacion de flags");
						System.out.println("El estado de Reparo KGR aun no se encuentra calculado. Omitiendo la actualizacion de flags");
					}
				}

			}	

		} catch (SQLException e) {
			System.out.println("No se puede consultar mensajes: "+e);
		}
		
		
		connKondor1.closeConnection( log);

	}

	private static boolean isDealProcessing(int dealId, int transactionId, Stack stack) {	
		Iterator it = stack.iterator();
		boolean ret = false;

		System.out.println("elementos en la pila: " + stack.size());

		while (it.hasNext()) {
			d = (Deal) it.next();
			int dealIdStack = d.getDealId();
			int transactionIdStack = d.getTransactionId();

			if (dealIdStack == dealId && transactionIdStack == transactionId) {
				ret = true;
				System.out.println("Deals procesando: " + dealIdStack + " - " + transactionId);
				break;
			}
			
		}
		return ret;
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
			con.closeConnection(log);
		}
			
		}
	
}
