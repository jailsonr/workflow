package cl.security.mdd.utils;

import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Properties;
import org.apache.log4j.Logger;

public class LoadProperties
{
	 public HashMap getProperties(String name, Logger log) {
	     FileInputStream file = null;
	     
	     try {
	       file = new FileInputStream(name);
	       Properties propers = new Properties();
	       
	       try {
	         propers.load(file);
	         
	         try {
	           file.close();
	         }
	         catch (IOException ex) {
	           
	           log.info("Problemas al Leer las Propiedades");
	         } 
	         if (propers.isEmpty())
	         {
	           log.info("Propiedades vacías");
	         }
	       }
	       catch (IOException ex) {
	         
	         log.info("Problemas al Leer las Propiedades");
	       } finally {
	 
	         
	         try {
	           
	           file.close();
	         }
	         catch (IOException ex) {
	           
	           log.info("Problemas al Leer las Propiedades");
	         } 
	       } 
	       return new HashMap<Object, Object>(propers);
	     }
	     catch (FileNotFoundException ex) {
	       
	       log.error("El archivo de Propiedades no existe");
	       return null;
	     }
	     catch (IOException e) {
	       
	       log.error("Problemas al Leer las Propiedades");
	       return null;
	     }
	     catch (NullPointerException nulo) {
	       
	       log.error("Problemas al Leer las Propiedades");
	       
	       return null;
	     }
	     catch (Exception excep) {
	 
	       
	       log.error("Problemas al Leer las Propiedades");
	       return null;
	     } 
	   }
}
