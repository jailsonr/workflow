package cl.security.mdd.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.log4j.Logger;

public class ConnectionKondor {
	private static Connection conn = null;

	public static void openConnection(String url, Logger log) {

		try {
			Class.forName(Constants.DRIVER);
			url = Constants.URL;
			conn = DriverManager.getConnection(url);
			
			String[] user=url.split("=");
			user=user[1].split(";");
			System.out.println("Conectado a "+ user[0]);
			
			

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
		System.out.println("cerrando conexion a la base de datos");
		//log.info("cerrando conexion a la base de datos");

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
