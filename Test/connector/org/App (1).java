/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.kafka.metadata;

import com.ads.api.util.SystemManagerUtil;
import com.ads.mm.db.dao.DBEnvironment;
import com.ads.mm.db.dao.DBSystem;
import com.ads.mm.db.util.DBHelper;
import com.ads.mm.db.util.JSONUtil;
import com.icc.util.DBProperties;
import com.icc.util.RequestStatus;
import com.icc.util.SaferFile;
import java.io.FileReader;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import com.ads.api.beans.sm.SMEnvironment;
import com.ads.api.beans.common.AuditHistory;
import com.ads.api.beans.common.Node;
import com.ads.api.beans.kv.KeyValue;
import com.ads.api.beans.sm.SMSystem;
import com.ads.api.util.KeyValueUtil;
import com.ads.mm.db.dao.DBTable;
import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import com.icc.util.ApplicationUtil;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;

import org.apache.log4j.Logger;

/**
 *
 * @author RKeshabo
 */
public class App {

    private static final Logger LOGGER = Logger.getLogger(App.class);
    private static StringBuilder message = null;
    private static Map<String, DBTable> tableMap = null;
    private static LinkedTreeMap<String, Object> typemap = null;
    private static LinkedTreeMap<String, Object> childPropertiesMap = null;
    /**
     *
     * @param systemManagerUtil
     * @param systemName
     * @param envName
     * @param filePath
     * @param log
     * @param keyValueUtil
     * @param systemId
     * @param environmentId
     * @return
     */
    public static String catLauncher(SystemManagerUtil systemManagerUtil, String systemName, String envName, String filePath, StringBuilder log, KeyValueUtil keyValueUtil, int systemId, int environmentId) {

        List<File> processingfiles = getprocessingFiles(filePath);

        List<File> processingfilesv = getRequiredFilesForProcess(processingfiles);
        RequestStatus requestStatus = null;
        message = log;
        for (File file : processingfilesv) {
            String absolutePath = file.getAbsolutePath();

            Map<String, Object> jsonSchemaMap = null;
            String rootTableName = absolutePath.substring(absolutePath.lastIndexOf(ApplicationUtil.AMM_FILE_SEPERATOR) + 1, absolutePath.lastIndexOf("."));

            try {
                if (StringUtils.isNotBlank(absolutePath)) {
                    JSONParser parser = new JSONParser();
                    try (FileReader fileReader = new FileReader(new SaferFile(absolutePath))) {
                        Object obj = parser.parse(fileReader);
                        JSONObject jsonObject = (JSONObject) obj;
                        if (!jsonObject.isEmpty()) {
                            jsonSchemaMap = new Gson().fromJson(jsonObject.toString(), HashMap.class);
                        }
                    }
                }

                DBEnvironment dbEnvironment = JSONUtil.loadEnvironment(systemId, envName, "null", absolutePath, DBHelper.ModelType.PHYSICAL);
                dbEnvironment.setSystem(new DBSystem(systemId));
                dbEnvironment.setEnvironmentId(environmentId);
                dbEnvironment.setEnvironmentType(DBEnvironment.DatabaseType.JSON);
                dbEnvironment.setDatabseType("JSON");
                requestStatus = systemManagerUtil.updateEnvironmentMetadata(dbEnvironment, DBProperties.Operation.UPDATE_APPEND, true);

                tableMap = dbEnvironment.getTableMap();
                for (Map.Entry<String, DBTable> entry : tableMap.entrySet()) {
                    String tablePath = entry.getKey();
                    DBTable dbTable = entry.getValue();
                    String tableName = dbTable.getTableName();

                    if (jsonSchemaMap.get("properties") != null && !jsonSchemaMap.get("properties").equals("") && !rootTableName.equals(tableName)) {
                        LinkedTreeMap<String, Object> propertiesMap = (LinkedTreeMap) jsonSchemaMap.get("properties");
                        updateKeyValueUtil(tableName, propertiesMap, systemName, envName, systemManagerUtil, keyValueUtil);
                    } else {
                        if (jsonSchemaMap.get("type") != null) {
                            String extendpropertyValue = (String) jsonSchemaMap.get("type");
                            createtableExtendPro(rootTableName, extendpropertyValue, systemName, envName, systemManagerUtil, keyValueUtil);
                        }

                    }
                }






            } catch (Exception ex) {
                LOGGER.warn("Inside catLauncher() " + ex);
                LOGGER.error(ex);
                StringWriter exceptionLog = new StringWriter();
                ex.printStackTrace(new PrintWriter(exceptionLog));
                message.append("Exception In catLauncher() \n" + exceptionLog.toString());
                message.append("\n ================================");
            }
        }
        return message.append(requestStatus.getStatusMessage()).toString();
    }
   

    
    private static void updateKeyValueUtil(String tableName, Map<String, Object> propertiesMap, String systemName, String envName, SystemManagerUtil systemManagerUtil, KeyValueUtil keyValueUtil) {

        String extendpropertyValue;
    ArrayList<LinkedTreeMap<String,String>> list = new ArrayList<>();    
        if (propertiesMap.get(tableName) != null) {
            typemap = (LinkedTreeMap) propertiesMap.get(tableName);
            if (typemap.get("type") != null) {
                extendpropertyValue = (String) typemap.get("type");
                if(typemap.get("items")!=null)
                {
                list=  (ArrayList<LinkedTreeMap<String, String>>) typemap.get("items");
               
                LinkedTreeMap<String,String> map =list.get(0);
                String extnded=map.get("type");
                if(extnded.equals("object"))
                {
                 extendpropertyValue=extendpropertyValue+"<struct>";
                }
                else if(extnded.equals("string"))
                {
                     extendpropertyValue=extendpropertyValue+"[string]";
                }
                else if(extnded.equals("int"))
                {
                     extendpropertyValue=extendpropertyValue+"[int]";
                }
                }
                childPropertiesMap = (LinkedTreeMap) typemap.get("properties");
                createtableExtendPro(tableName, extendpropertyValue, systemName, envName, systemManagerUtil, keyValueUtil);
            }
        } else if (childPropertiesMap.get(tableName) != null) {

            updateKeyValueUtil(tableName, childPropertiesMap, systemName, envName, systemManagerUtil, keyValueUtil);

        }
    }

    /**
     *
     * @param systemId
     * @param environentName
     * @param systemManagerUtil
     * @return
     */
    public static int metadataEnvironmentCreation(int systemId, String environentName, SystemManagerUtil systemManagerUtil) {
        int environmentId = -1;
        try {
            if (systemManagerUtil.getEnvironmentId(systemId, environentName) == -1) {
                environmentId = createEnvironment(systemId, environentName, systemManagerUtil);
            } else {
                environmentId = systemManagerUtil.getEnvironmentId(systemId, environentName);
            }
        } catch (Exception e) {
            environmentId = createEnvironment(systemId, environentName, systemManagerUtil);
        }
        return environmentId;
    }

    /**
     *
     * @param systemId
     * @param environentName
     * @param systemManagerUtil
     * @return
     */
    public static int createEnvironment(int systemId, String environentName, SystemManagerUtil systemManagerUtil) {
        int environmentId = -1;
        try {
            SMEnvironment environment = new SMEnvironment();
            environment.setSystemId(systemId);
            AuditHistory auditHistory = new AuditHistory();
            auditHistory.setCreatedBy("Administrator");
            environment.setAuditHistory(auditHistory);
            environment.setSystemEnvironmentType(environentName);
            environment.setSystemEnvironmentName(environentName);
            LOGGER.info("EnvironmentName " + environentName);
            environment.setDatabaseType("JSON");
            environment.setFieldDelimiter(",");
            environment.setRowDelimiter("\n");
            RequestStatus requestStatus = systemManagerUtil.createEnvironment(environment);
            LOGGER.info("Metadata Creation is created or not " + requestStatus.getStatusMessage());
            environmentId = systemManagerUtil.getEnvironmentId(systemId, environentName);
        } catch (Exception e) {
            LOGGER.error(e);
            StringWriter exceptionLog = new StringWriter();
            e.printStackTrace(new PrintWriter(exceptionLog));
            message.append("Exception In createEnvironment() \n" + exceptionLog.toString());
            message.append("\n ================================");
        }
        return environmentId;
    }

    /**
     *
     * @param systemName
     * @param systemManagerUtil
     * @param environmentName
     * @return
     */
    public static int metadataSystemCreation(String systemName, SystemManagerUtil systemManagerUtil, String environmentName) {
        int systemId = 0;
        try {
            if (systemManagerUtil.getSystemId(systemName) == 0) {
                systemId = createSystem(systemName, systemManagerUtil);
            }
        } catch (Exception e) {
            LOGGER.error(e);
            StringWriter exceptionLog = new StringWriter();
            e.printStackTrace(new PrintWriter(exceptionLog));
            message.append("Exception In metadataSystemCreation() \n" + exceptionLog.toString());
            message.append("\n ================================");
        }
        return systemId;
    }

    /**
     *
     * @param systemName
     * @param systemManagerUtil
     * @return
     */
    public static int createSystem(String systemName, SystemManagerUtil systemManagerUtil) {
        int systemId = -1;
        try {
            SMSystem system = new SMSystem();
            system.setSystemName(systemName);
            RequestStatus req = systemManagerUtil.createSystem(system);
            systemId = (systemManagerUtil.getSystemId(systemName) == 0) ? systemId : systemManagerUtil.getSystemId(systemName);
        } catch (Exception e) {
            LOGGER.error(e);
            StringWriter exceptionLog = new StringWriter();
            e.printStackTrace(new PrintWriter(exceptionLog));
            message.append("Exception In createSystem() \n" + exceptionLog.toString());
            message.append("\n ================================");
        }
        return systemId;
    }

    /**
     *
     * @param tableName
     * @param extendpropertyValue
     * @param systemNme
     * @param environmentNam
     * @param systemManagerUtil
     * @param keyValueUtil
     */
    public static void createtableExtendPro(String tableName, String extendpropertyValue, String systemNme, String environmentNam, SystemManagerUtil systemManagerUtil, KeyValueUtil keyValueUtil) {
        try {
            int tableId = systemManagerUtil.getTableId(systemNme, environmentNam, tableName);
            KeyValue keyValue = new KeyValue();
            keyValue.setKey("Type");
            keyValue.setValue(extendpropertyValue);
            keyValue.setPublished(true);
            keyValue.setVisibility(1);
            RequestStatus requestStatus = keyValueUtil.addKeyValue(keyValue, Node.NodeType.SM_TABLE, tableId);

        } catch (Exception e) {
            LOGGER.error(e);
            StringWriter exceptionLog = new StringWriter();
            e.printStackTrace(new PrintWriter(exceptionLog));
            message.append("Exception In createtableExtendPro() \n" + exceptionLog.toString());
            message.append("\n ================================");
        }
    }

    public static List<File> getprocessingFiles(String directoryName) {
        File directory = new File(directoryName);
        List<File> resultList = new ArrayList<>();

        File[] fList = directory.listFiles();
        resultList.addAll(Arrays.asList(fList));
        for (File filev : fList) {
            if (filev.isFile()) {
            } else if (filev.isDirectory()) {
                resultList.addAll(getprocessingFiles(filev.getAbsolutePath()));
            }
        }
        return resultList;
    }

    private static List<File> getRequiredFilesForProcess(List<File> processingFiles) {
        List<File> resultListv = new ArrayList<>();

        processingFiles.stream().filter((file) -> (file.getAbsolutePath().contains("."))).filter((file) -> (file.getName().split("\\.")[1].contains("json"))).forEachOrdered((file) -> {
            try {
                resultListv.add(file.getCanonicalFile());
            } catch (IOException ex) {
                Logger.getLogger(App.class.getName()).log(Level.FATAL, null, ex);
            }
        });
        return resultListv;
    }
}
