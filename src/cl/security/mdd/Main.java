package cl.security.mdd;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import cl.security.mdd.dao.MessagesDAO;
import cl.security.mdd.utils.Constants;


public class Main {

	
	public static void main (String...args) throws InterruptedException {
		//CustomWindowsUpdater cwu = new CustomWindowsUpdater();
		
		PropertyConfigurator.configure(Constants.LOG4J);
		Logger log = Logger.getLogger(Main.class);
		
		MessagesDAO m= new MessagesDAO();
		
		
		log.info("-------------------------------------");
		log.info("----------Inicio Workflow------------");
		log.info("-------------------------------------");	
		
		
		
		while(true) {
			Thread.sleep(5000);
			m.messagesInProgress();
		}
		
	}
	
	
	
}
