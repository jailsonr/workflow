package cl.security.mdd.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.apache.log4j.Logger;


public class ConnectionKGR {
	
	private static Connection conn = null;
	
	public static void openConnection(String url, Logger log) {

		try {
			Class.forName(Constants.DRIVER);
			url = Constants.URL2;
			conn = DriverManager.getConnection(url);

			System.out.println("Conectado a KGR");

		} catch (SQLException ex) {
			log.info("No es posible acceder a la base de datos: " + ex);
		} catch (ClassNotFoundException ex) {
			log.info("No es posible encontrar el Driver de conexión: " + ex);
		}
	}

	
	public Connection getConnection() {
		return conn;
	}

	
	public void closeConnection(Logger log) {
		log.info("cerrando conexión a la base de datos");

		try {
			if (conn != null) {
				conn.close();
			} else {
				System.out.println("No existen conexiones por cerrar");
			}
		} catch (SQLException ex) {
			log.warn("No es posible cerrar la conexion: " + ex);
		}
	}
}
