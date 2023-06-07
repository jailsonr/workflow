package cl.security.mdd.utils;

import java.io.InputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import org.apache.log4j.Logger;

public class ExecuteKondorServices {
	private String KIreturn;

	public String KplusImport(String File, Logger log) {

		String[] comandos = { "KplusImport", "-f", Constants.KPLUSIMPORT, "FILE", File };
		Runtime runtime = Runtime.getRuntime();
		Process process = null;

		log.info("Iniciar ejecución KplusImport");

		try {
			log.info("Comenzando ejecución");
			process = runtime.exec(comandos);
		} catch (IOException ex) {
			log.error("Error en la ejecución del comando: " + comandos);
			log.error("Motivo: " + ex);
		}

		InputStream is = process.getInputStream();
		InputStreamReader isr = new InputStreamReader(is);
		BufferedReader br = new BufferedReader(isr);
		String line = null;
		
		try {
			while((line = br.readLine()) != null) {
				this.KIreturn = String.valueOf(this.KIreturn) + line + "\n";
			}
		}catch(IOException ex) {
			log.error("Error al obtener los resultados");
			log.error(""+ex);
		}

		log.info("KIreturn : "+this.KIreturn);
		return this.KIreturn;
	}
}
