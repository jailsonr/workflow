package cl.security.mdd.dao;

import java.util.Iterator;
import java.io.Writer;
import java.io.FileNotFoundException;
import java.io.Reader;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;
import java.util.Calendar;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.sql.ResultSet;
import java.sql.CallableStatement;
import java.io.PrintWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import org.apache.log4j.Logger;
import java.util.HashMap;

import org.apache.log4j.Logger;

import cl.security.mdd.dto.KISFile;
import cl.security.mdd.utils.CallingSP;
import cl.security.mdd.utils.ConnectionKondor;
import cl.security.mdd.utils.Constants;

public class KisFileDAO {
	 public static final String DATE_FORMAT = "dd-MM-yyyy_HH:mm:ss.SSSSSS";
	    private String fileName;
	    private String routeKisFile;
	    private String routeTemplates;
	    private int paramId;
	    private String paramName;
	    private String paramValue;
	    private int dealId;
	    private HashMap<Integer, KISFile> mapas;
	    
	    Constants cons = new Constants();
	    
	    
	    public String importFile(int dealId, int kdbTableId, int KGRStatus, 
	    		String KGRRequest, ConnectionKondor connKondor, Logger log) {
	       
	    	this.dealId = dealId;
	        final FileWriter fw = null;
	        final PrintWriter salida = null;
	        KGRRequest = null;
	        
	        
	        final String storeProcedure = "{call Kustom.."+CallingSP.KISFIELDS+"(?,?,?)}";
	        
	        CallableStatement cs = null;
	        ResultSet rs = null;
	        
	        try {
	        	connKondor.openConnection(Constants.URL,log);
	            cs = connKondor.getConnection().prepareCall(storeProcedure);
	        }
	        catch (SQLException e) {
	            log.error("No se puede generar la LLamada al procedimiento");
	            log.error("Motivo : " + e.getMessage());
	        }
	        log.info("---------------------------------------------");
	        log.info("---------Inicia Creacion KisFile-------------");
	        log.info("---------------------------------------------");
	        try {
	            cs.setInt(1, kdbTableId);
	        }
	        catch (SQLException e) {
	            log.error("No se pudo Setear el KDBTable");
	            log.error("Motivo : " + e.getMessage());
	        }
	        try {
	            cs.setInt(2, dealId);
	        }
	        catch (SQLException e) {
	            log.error("No se pudo Setear el DealId");
	            log.error("Motivo : " + e.getMessage());
	        }
	        try {
	            cs.setString(3, KGRRequest);
	        }
	        catch (SQLException e) {
	            log.error("No se pudo Setear KGRRequest");
	            log.error("Motivo : " + e.getMessage());
	        }
	        
	        log.info("KdbTableId : " + kdbTableId);
	        log.info("DealId : " + dealId);
	        log.info("KGRRequest : " + KGRRequest);
	        
	        try {
	        	rs = cs.executeQuery();
	        }
	        catch (SQLException e2) {
	            log.error("Error al obtener el ResultSet");
	            log.error("Raz\u00f3n : " + e2.getMessage());
	        }
	        int i = 0;
	        this.mapas = new HashMap<Integer, KISFile>();
	        if (rs != null) {
	            try {
	                while (rs.next()) {
	                    this.paramId = rs.getInt("ParamId");
	                    this.paramName = rs.getString("ParamName").trim();
	                    this.paramValue = rs.getString("ParamValue").trim();
	                    log.info("ParamId : " + this.paramId);
	                    log.info("ParamName : " + this.paramName);
	                    log.info("ParamValue : " + this.paramValue);
	                    final KISFile parametros = new KISFile();
	                    parametros.setParamId(this.paramId);
	                    parametros.setParamName(this.paramName);
	                    parametros.setParamValue(this.paramValue);
	                    this.mapas.put(i, parametros);
	                    ++i;
	                }
	            }
	            catch (SQLException e3) {
	                log.error("No se pudo obtener los Resultados del ResultSet");
	                log.error("Raz\u00f3n : " + e3.getMessage());
	            }
	        }
	        else {
	            log.info("No existen Resultados");
	        }
	        try {
	            this.fileName = this.createKISFile(this.mapas, dealId, log);
	        }
	        catch (IOException e4) {
	            log.error("No se pudo generar la instancia para la creaci\u00f3n del archivo KIS");
	            log.error("Raz\u00f3n : " + e4.getMessage());
	        }
	        try {
	            cs.close();
	        }
	        catch (SQLException e3) {
	            log.error("No se pudo finalizar la ejecuci\u00f3n del Procedimiento");
	            log.error("Raz\u00f3n : " + e3.getMessage());
	        }
	        return this.fileName;
	    }
	    
	    
	    public String createKISFile(HashMap<Integer, KISFile> mapa, int dealId, Logger log) throws IOException {
	        String template = null;
	        String dealId2 = null;
	        System.out.println("Mapa: "+mapa.size());
	        if (mapa.size() == 3) {
	        	
	            for (final Map.Entry<Integer, KISFile> e : mapa.entrySet()) {
	                final Integer Id = e.getKey();
	                final KISFile file = e.getValue();
	                final int getParamId = file.getParamId();
	                final String getParamName = file.getParamName();
	                final String getParamValue = file.getParamValue();
	                
	                System.out.println("File: "+file);
	                log.info("File:"+file);
	                
	                
	                if (getParamName.equals("NbParams")) {
	                    final String nbParams = getParamValue; 
	                    System.out.println(nbParams);
	                    log.info(nbParams);
	                }
	                
	                
	                else if (getParamName.equals("Template")) {
	                    template = getParamValue;
	                    System.out.println("Template: "+template);
	                    log.info("Template: "+template);
	                }
	                else {
	                    if (!getParamName.equals("DealId")) {
	                    	 System.out.println("!DealId: "+getParamName);
	                        continue;
	                    }
	                    dealId2 = getParamValue;
	                    System.out.println("DealId2: "+dealId2);
	                }
	            }
	        }
	       
	        
	        this.routeKisFile = cons.ROUTEKIS;
	        this.routeTemplates = cons.ROUTETEMPLATES;
	        final SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy_HH:mm:ss.SSSSSS");
	        final Calendar cal = Calendar.getInstance();
	        final String Date = sdf.format(cal.getTime());
	        final String FileNameCreate = String.valueOf(this.routeKisFile) + template + "_" + dealId + "_" + Date;
	        log.info("Archivo: "+FileNameCreate);
	        log.info("ID-Nuevo archivo Kis : " + FileNameCreate);
	        log.info("ID-Archivo Template  : " + this.routeTemplates + template);
	        BufferedReader reader = null;
	        PrintWriter writer = null;
	        String line = null;
	        final File In = new File(String.valueOf(this.routeTemplates) + template);
	        System.out.println(routeKisFile);
	        System.out.println(routeTemplates);
	        
	        log.info(routeKisFile);
	        log.info(routeTemplates);
	        final File Out = new File(FileNameCreate);
	        try {
	            reader = new BufferedReader(new FileReader(In));
	        }
	        catch (FileNotFoundException e2) {
	            log.error("Error a leer el Template");
	            log.error("Raz\u00f3n :" + e2.getMessage());
	        }
	        writer = new PrintWriter(new FileWriter(Out));
	        while ((line = reader.readLine()) != null) {
	            writer.println(line.replaceAll("#DealId#", dealId2));
	        }
	        try {
	            reader.close();
	        }
	        catch (IOException e3) {
	            log.error("No se pudo finalizar la lectura del archivo");
	            log.error("Raz\u00f3n : " + e3.getMessage());
	        }
	        writer.close();
	        return FileNameCreate;
	    }
	    
	    public int getKISDealId(ConnectionKondor connKondor, Logger log, int kdbTableId, int dealId) {
	        final String storeProcedure = "{call "+CallingSP.GETKISDEAL+"(?,?,?)}";
	        CallableStatement cs = null;
	        try {
	        	connKondor.openConnection(Constants.URL, log);
	            cs = connKondor.getConnection().prepareCall(storeProcedure);
	        }
	        catch (SQLException e) {
	            log.error("No se pudo ejecutar el procedimiento WKF_getKISDealId");
	            log.error("Raz\u00f3n: " + e.getMessage());
	        }
	        try {
	            cs.setInt(1, kdbTableId);
	        }
	        catch (SQLException e) {
	            log.error("SQL Error   : " + e.getSQLState());
	            log.error("Traza Error : " + e.getStackTrace());
	        }
	        try {
	            cs.setInt(2, dealId);
	        }
	        catch (SQLException e) {
	            log.error("No se pudo setear el valor del KdbTableId");
	            log.error("Motivo: " + e.getMessage());
	        }
	        try {
	            cs.registerOutParameter(3, 4);
	        }
	        catch (SQLException e) {
	            log.error("No se pudo obtener el valor del Salida Output");
	            log.error("Motivo: " + e.getMessage());
	        }
	        try {
	            cs.execute();
	        }
	        catch (SQLException e) {
	            log.error("No se pudo ejecutar el SP");
	            log.error("Motivo : " + e.getMessage());
	        }
	        int dealsIdOut = 0;
	        try {
	            dealsIdOut = cs.getInt(3);
	            log.info("Resultado de la ejecucion de WKF_getKISDealId [KdbTables_Id,Deals_Id_In,Deals_Id_Out]: [" + kdbTableId + "," + dealId + "," + dealsIdOut + "]");
	        }
	        catch (SQLException e2) {
	            log.error("No se pudo obtener el valor MLSStatus");
	            log.error("Motivo " + e2.getMessage());
	            dealsIdOut = dealId;
	        }
	        return dealsIdOut;
	    }
}
