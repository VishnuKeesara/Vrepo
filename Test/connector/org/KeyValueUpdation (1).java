/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.bulk.extendedproperties.updation.v1;

import com.ads.api.beans.common.Node;
import com.ads.api.beans.kv.KeyValue;
import com.ads.api.util.KeyValueUtil;
import java.sql.Connection;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;

/**
 *
 * @author BEarla
 */
public class KeyValueUpdation {

    private static final Logger logger = Logger.getLogger(KeyValueUpdation.class);

    public static String bulkKeyValueUpdation(Connection con, KeyValueUtil keyValueUtil, KeyValue keyValue, String updatevalue, String objectTypeId, String objectId, Node.NodeType nodeType) {
        String statusMessage = "";
        try {
            if (keyValue.getValueRefId() == null) {
                if (keyValue.getType().equalsIgnoreCase("COMBO BOX") || keyValue.getType().equalsIgnoreCase("LIST")
                        || keyValue.getType().equalsIgnoreCase("RADIO")) {
                    List<KeyValue> subkeyValuesList = keyValue.getKeyValues();
                    boolean addFlag = false;
                    for (KeyValue subKeyValue : subkeyValuesList) {
                        if (subKeyValue.getKey().equalsIgnoreCase(updatevalue)) {
                            updatevalue = subKeyValue.getValue();
                            addFlag = true;
                            break;
                        }
                    }

                    if (addFlag) {
                        KeyValue Keyvalue1 = new KeyValue();
                        Keyvalue1.setUIType(keyValue.getUIType());
                        Keyvalue1.setSubKeyValues(keyValue.getKeyValues());
                        Keyvalue1.setKey(keyValue.getKey());
                        Keyvalue1.setValue(updatevalue);
                        Keyvalue1.setPublished(true);
                        Keyvalue1.setVisibility(1);
                        Keyvalue1.setObjectId(objectId);
                        Keyvalue1.setObjectTypeId(objectTypeId);
                        statusMessage = keyValueUtil.updateKeyValue(keyValue).getStatusMessage();
                    }

                } else {
                    statusMessage = keyValueUtil.updateKeyValue(String.valueOf(keyValue.getId()), updatevalue, objectTypeId, objectId).getStatusMessage();
                }
//                return statusMessage;
                if (statusMessage.contains("Fail")) {
                    Map<String, String> keyValues = keyValueUtil.getKeyValues(objectTypeId, "-1");
                    boolean keyFlag = keyValues.containsKey(keyValue.getKey());

                    if (keyFlag) {
                        if (keyValue.getType().equalsIgnoreCase("Text Box") || keyValue.getType().equalsIgnoreCase("NUMBER")
                                || keyValue.getType().equalsIgnoreCase("DATE PICKER") || keyValue.getType().equalsIgnoreCase("RICH EDITOR")
                                || keyValue.getType().equalsIgnoreCase("LABEL") || keyValue.getType().equalsIgnoreCase("CHECK BOX")
                                || keyValue.getType().equalsIgnoreCase("COLOR PICKER") || keyValue.getType().equalsIgnoreCase("BOOLEAN")) {

                            KeyValue Keyvalue2 = new KeyValue();
                            Keyvalue2.setUIType(keyValue.getUIType());
//                            Keyvalue2.setSubKeyValues(keyValue.getKeyValues());
                            Keyvalue2.setKey(keyValue.getKey());
                            Keyvalue2.setValue(updatevalue);
                            Keyvalue2.setPublished(true);
                            Keyvalue2.setVisibility(1);

                            statusMessage = keyValueUtil.addKeyValue(Keyvalue2, nodeType, Integer.parseInt(objectId)).getStatusMessage();

                        } else if (keyValue.getType().equalsIgnoreCase("COMBO BOX") || keyValue.getType().equalsIgnoreCase("LIST")
                                || keyValue.getType().equalsIgnoreCase("RADIO")) {
                            List<KeyValue> subkeyValuesList = keyValue.getKeyValues();
                            boolean addFlag = false;
                            for (KeyValue subKeyValue : subkeyValuesList) {
                                if (subKeyValue.getKey().equalsIgnoreCase(updatevalue)) {
                                    updatevalue = subKeyValue.getValue();
                                    addFlag = true;
                                    break;
                                }
                            }

                            if (addFlag) {
                                KeyValue Keyvalue1 = new KeyValue();
                                Keyvalue1.setUIType(keyValue.getUIType());
                                Keyvalue1.setSubKeyValues(keyValue.getKeyValues());
                                Keyvalue1.setKey(keyValue.getKey());
                                Keyvalue1.setValue(updatevalue);
                                Keyvalue1.setPublished(true);
                                Keyvalue1.setVisibility(1);

                                statusMessage = keyValueUtil.addKeyValue(Keyvalue1, nodeType, Integer.parseInt(objectId)).getStatusMessage();
                            } else {
                                return keyValue.getKey() + " is enabled with data Manager but the value is not found in Data Manager.Give the value that is Present in Data Manager Module to update it";
                            }
                        }
                    }
                }
                return statusMessage;
            } else {
                String rdmColumnId = KeyValueUpdationUtility.getRDMColumnID(con, keyValue.getConfigObject().getMcId());
                if (rdmColumnId != null) {
                    Map<String, String> rdmColumnValuesAgainstIds = KeyValueUpdationUtility.getRdmColumnValuesAssociatedWithExtendedProperty(con, rdmColumnId);
                    if (rdmColumnValuesAgainstIds.containsKey(updatevalue)) {
                        statusMessage = keyValueUtil.updateKeyValue(String.valueOf(keyValue.getId()), rdmColumnValuesAgainstIds.get(updatevalue).split("##")[0], objectTypeId, objectId).getStatusMessage();
//                        return statusMessage;
                        if (statusMessage.contains("Fail")) {
                            Map<String, String> keyValues = keyValueUtil.getKeyValues(objectTypeId, "-1");
                            boolean keyFlag = keyValues.containsKey(keyValue.getKey());

                            if (keyFlag) {
                                if (keyValue.getType().equalsIgnoreCase("Text Box") || keyValue.getType().equalsIgnoreCase("NUMBER")
                                        || keyValue.getType().equalsIgnoreCase("DATE PICKER") || keyValue.getType().equalsIgnoreCase("RICH EDITOR")
                                        || keyValue.getType().equalsIgnoreCase("LABEL") || keyValue.getType().equalsIgnoreCase("CHECK BOX")
                                        || keyValue.getType().equalsIgnoreCase("COLOR PICKER") || keyValue.getType().equalsIgnoreCase("BOOLEAN")) {

                                    KeyValue Keyvalue2 = new KeyValue();
                                    Keyvalue2.setUIType(keyValue.getUIType());
//                            Keyvalue2.setSubKeyValues(keyValue.getKeyValues());
                                    Keyvalue2.setKey(keyValue.getKey());
                                    Keyvalue2.setValue(updatevalue);
                                    Keyvalue2.setPublished(true);
                                    Keyvalue2.setVisibility(1);

                                    statusMessage = keyValueUtil.addKeyValue(Keyvalue2, nodeType, Integer.parseInt(objectId)).getStatusMessage();

                                } else if (keyValue.getType().equalsIgnoreCase("COMBO BOX") || keyValue.getType().equalsIgnoreCase("LIST")
                                        || keyValue.getType().equalsIgnoreCase("RADIO")) {
                                    List<KeyValue> subkeyValuesList = keyValue.getKeyValues();
                                    boolean addFlag = false;
                                    for (KeyValue subKeyValue : subkeyValuesList) {
                                        if (subKeyValue.getKey().equalsIgnoreCase(updatevalue)) {
                                            updatevalue = subKeyValue.getValue();
                                            addFlag = true;
                                            break;
                                        }
                                    }

                                    if (addFlag) {
                                        KeyValue Keyvalue1 = new KeyValue();
                                        Keyvalue1.setUIType(keyValue.getUIType());
                                        Keyvalue1.setSubKeyValues(keyValue.getKeyValues());
                                        Keyvalue1.setKey(keyValue.getKey());
                                        Keyvalue1.setValue(rdmColumnValuesAgainstIds.get(updatevalue).split("##")[0]);
                                        Keyvalue1.setPublished(true);
                                        Keyvalue1.setVisibility(1);

//                                        Keyvalue1.setSelectedValue(updatevalue);
                                        statusMessage = keyValueUtil.addKeyValue(Keyvalue1, nodeType, Integer.parseInt(objectId)).getStatusMessage();
                                    } else {
                                        return keyValue.getKey() + " is enabled with data Manager but the value is not found in Data Manager.Give the value that is Present in Data Manager Module to update it";
                                    }
                                }
                            }
                            return statusMessage;
                        } else {
                            return statusMessage;
                        }

                    } else {
                        statusMessage = keyValue.getKey() + " is enabled with Reference data Manager but the value is not found in Refernce Data Manager.Give the value that is Present in Reference Data Manager Module to update it";
                        return statusMessage;
                    }
                }
                return keyValue.getKey() + "is enable with Reference data Manager and could able to get Reference DataManager Column ID ";
            }
        } catch (Exception e) {
            logger.error("Exception Occured In bulkKeyValueUpdation() ", e);
            statusMessage = "Exception Occured In bulkKeyValueUpdation " + e.getMessage();
            return statusMessage;
        }
    }
}
