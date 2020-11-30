package org.monba.service.impl;

import com.mongodb.client.FindIterable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;
import org.monba.service.AdgroupService;
import org.monba.utils.HbaseUtil;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service("adgroupService")
public class AdgroupServiceImpl implements AdgroupService {
    int count = 0;
    Table meta_adgroup = null;

    @Resource
    private MongoTemplate mongoTemplate;


    @Override
    public void getData() {

        //在这里获取hbase的连接
        Connection hbaseConnection = HbaseUtil.getHbaseConnection();
        try {
            meta_adgroup = hbaseConnection.getTable(TableName.valueOf("META_ADGROUP"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        Map<Long, JSONObject> map = new HashMap<>();
        FindIterable<Document> documents = mongoTemplate.getCollection("meta_adgroup").find();
        for(Document document : documents) {
            if(document == null) {
                continue;
            }
            JSONObject jsonObject = new JSONObject(document.toJson());
            Long ad_id = jsonObject.getLong("ad_id");
            map.put(ad_id, jsonObject);
            if(map.keySet().size() >= 10000) {
                transmit(new ArrayList<JSONObject>(map.values()));
                map.clear();
            }
        }


        if(!map.isEmpty()) {
            transmit(new ArrayList<JSONObject>(map.values()));
        }
        HbaseUtil.close(hbaseConnection);
    }

    private void transmit(List<JSONObject> list) {
        count++;
        System.out.println("正在处理第："+count+"万条数据");



        for (JSONObject jsonObject : list) {
            long ad_id = jsonObject.getLong("ad_id");  //列名为：ad_id
            String cert = jsonObject.getString("cert"); //列名为：cert
            long cp_id = jsonObject.getLong("cp_id");   //列名为：cp_id
            long org_id = jsonObject.getLong("org_id"); //列名为：org_id
            long ts = jsonObject.getLong("ts");         //列名为：ts
            boolean automatedKeywordsOptIn = jsonObject.getBoolean("automatedKeywordsOptIn");   //列名为：automatedKeywordsOptIn

            //为空判断
            boolean cpaGoalIsNullIsNull = jsonObject.isNull("cpaGoal");
            double cpaGoal_Amount;//列名为：cpaGoal
            if(cpaGoalIsNullIsNull){
                cpaGoal_Amount = 0.0;    //列名为：dailyBudgetAmount
            }else{
                JSONObject cpaGoalJSON = jsonObject.getJSONObject("cpaGoal");
                cpaGoal_Amount = cpaGoalJSON.getDouble("amount");
            }

            JSONObject defaultCpcBidJSON = jsonObject.getJSONObject("defaultCpcBid");
                //列名为：defaultCpcBid_amount
            double defaultCpcBid_Amount = defaultCpcBidJSON.getDouble("amount");

            boolean deleted = jsonObject.getBoolean("deleted"); //列名为：deleted
            String displayStatus = jsonObject.getString("displayStatus");   //列名为：displayStatus

            //为空判断
            boolean endTimeIsNull = jsonObject.isNull("endTime");
            String endTime;             //列名为：endTime
            if(endTimeIsNull){
                endTime = "";    //列名为：dailyBudgetAmount
            }else{
                endTime = jsonObject.getString("endTime");
            }

            String modificationTime = jsonObject.getString("modificationTime"); //列名为：modificationTime
            String name = jsonObject.getString("name"); //列名为：name

            //列名为：servingStateReasons
            //该列可能有值也可能没有值
            //为空判断
            boolean servingStateReasonsIsNull = jsonObject.isNull("servingStateReasons");
            String servingStateReasons;
            if(servingStateReasonsIsNull){
                servingStateReasons = "";    //列名为：dailyBudgetAmount
            }else{
                JSONArray JJSONArray = jsonObject.getJSONArray("servingStateReasons");
                JSONArray jsonArrayK = new JSONArray();
                for (Object o : JJSONArray) {
                    jsonArrayK.put(o);
                }
                servingStateReasons = jsonArrayK.toString();
            }


            String servingStatus = jsonObject.getString("servingStatus");   //列名为：servingStatus
            String startTime = jsonObject.getString("startTime");   //列名为：startTime
            String status = jsonObject.getString("status"); //列名为：status

            //targetingDimensions
            //为空判断
            boolean targetingDimensionsIsNull = jsonObject.isNull("targetingDimensions");
            int minAge;
            int maxAge;
            String gender;
            String country;
            String adminArea;
            String locality;
            String deviceClass;
            String daypart;
            String appDownloaders_included;
            String appDownloaders_excluded;
            if(targetingDimensionsIsNull){
                minAge = 0;                        //⬇️全是列名
                maxAge = 0;
                gender= "";
                country= "";
                adminArea= "";
                locality= "";
                deviceClass= "";
                daypart= "";
                appDownloaders_included= "";
                appDownloaders_excluded= "";      //⬆️
            }else{
                JSONObject targetingDimensions = jsonObject.getJSONObject("targetingDimensions");
                //age为空判断
                boolean ageIsNull = targetingDimensions.isNull("age");
                //如果age为空 则最小和 最大 都是0
                if(ageIsNull){
                    minAge = 0;
                    maxAge = 0;
                }else{
                    //如果age不为空， 则要判断 age中的included数组的两个json对象的值各自是否为空
                    JSONObject age = targetingDimensions.getJSONObject("age");
                    JSONArray included = age.getJSONArray("included");
                    JSONObject jsonObject1 = included.getJSONObject(0);

                    //如果minAge为空 则为0 不为空则getInt
                    if(jsonObject1.isNull("minAge")){
                        minAge=0;
                    }else{
                        minAge = jsonObject1.getInt("minAge");
                    }
                    //如果maxAge为空 则为0 不为空则getInt
                    if(jsonObject1.isNull("maxAge")){
                        maxAge=0;
                    }else{
                        maxAge = jsonObject1.getInt("maxAge");
                    }
                }
                //gender判空
                if(targetingDimensions.isNull("gender")){
                    gender = "";
                }else{
                    JSONObject genderJSONObject = targetingDimensions.getJSONObject("gender");
                    JSONArray includedJY = genderJSONObject.getJSONArray("included");
                    JSONArray jsonArrayX = new JSONArray();
                    for (Object o : includedJY) {
                        jsonArrayX.put(o);
                    }
                    gender = jsonArrayX.toString(); //列名为：gender
                }

                //country判空
                if(targetingDimensions.isNull("country")){
                    country = "";
                }else{
                    JSONObject countryJSONObject = targetingDimensions.getJSONObject("country");
                    JSONArray includedJY = countryJSONObject.getJSONArray("included");
                    JSONArray jsonArrayX = new JSONArray();
                    for (Object o : includedJY) {
                        jsonArrayX.put(o);
                    }
                    country = jsonArrayX.toString(); //列名为：country
                }

                //adminArea判空
                if(targetingDimensions.isNull("adminArea")){
                    adminArea = "";
                }else{
                    JSONObject adminAreaJSONObject = targetingDimensions.getJSONObject("adminArea");
                    JSONArray includedJY = adminAreaJSONObject.getJSONArray("included");
                    JSONArray jsonArrayX = new JSONArray();
                    for (Object o : includedJY) {
                        jsonArrayX.put(o);
                    }
                    adminArea = jsonArrayX.toString(); //列名为：adminArea
                }

                //locality判空
                if(targetingDimensions.isNull("locality")){
                    locality = "";
                }else{
                    JSONObject localityJSONObject = targetingDimensions.getJSONObject("locality");
                    JSONArray includedJY = localityJSONObject.getJSONArray("included");
                    JSONArray jsonArrayX = new JSONArray();
                    for (Object o : includedJY) {
                        jsonArrayX.put(o);
                    }
                    locality = jsonArrayX.toString(); //列名为：locality
                }

                //deviceClass判空
                if(targetingDimensions.isNull("deviceClass")){
                    deviceClass = "";
                }else{
                    JSONObject deviceClassJSONObject = targetingDimensions.getJSONObject("deviceClass");
                    JSONArray includedJY = deviceClassJSONObject.getJSONArray("included");
                    JSONArray jsonArrayX = new JSONArray();
                    for (Object o : includedJY) {
                        jsonArrayX.put(o);
                    }
                    deviceClass = jsonArrayX.toString(); //列名为：deviceClass
                }

                //daypart判空
                if(targetingDimensions.isNull("daypart")){
                    daypart = "";
                }else{
                    JSONObject daypartJSONObject = targetingDimensions.getJSONObject("daypart");
                    JSONObject userTimeJSONObject = daypartJSONObject.getJSONObject("userTime");
                    JSONArray includedJY = userTimeJSONObject.getJSONArray("included");
                    JSONArray jsonArrayX = new JSONArray();
                    for (Object o : includedJY) {
                        jsonArrayX.put(o);
                    }
                    daypart = jsonArrayX.toString(); //列名为：daypart
                }

                //daypart判空
                if(targetingDimensions.isNull("appDownloaders")){
                    appDownloaders_included = "";
                    appDownloaders_excluded = "";

                }else{
                    JSONObject appDownloadersJSONObject = targetingDimensions.getJSONObject("appDownloaders");
                   //   在这个里面要判断 appDownloaders下面的included是否为空
                    if(appDownloadersJSONObject.isNull("included")){
                       appDownloaders_included="";
                   }else{
                       JSONArray includedJY = appDownloadersJSONObject.getJSONArray("included");
                       JSONArray jsonArrayX = new JSONArray();
                       for (Object o : includedJY) {
                           jsonArrayX.put(o);
                       }
                       appDownloaders_included = jsonArrayX.toString(); //列名为：appDownloaders_included
                   }
                    //   在这个里面要判断 appDownloaders下面的excluded是否为空
                    if (appDownloadersJSONObject.isNull("excluded")){
                        appDownloaders_excluded="";
                    }else{
                        JSONArray excludedJY = appDownloadersJSONObject.getJSONArray("excluded");
                        JSONArray jsonArrayZ = new JSONArray();
                        for (Object o : excludedJY) {
                            jsonArrayZ.put(o);
                        }
                        appDownloaders_excluded = jsonArrayZ.toString(); //列名为：appDownloaders_excluded
                    }



                }

            }

            //hbase相关
            //设置rowKey
            Put put = new Put(Bytes.toBytes(ad_id));
            //添加每一个列
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("CERT"),Bytes.toBytes(cert));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("CP_ID"),Bytes.toBytes(cp_id));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("ORG_ID"),Bytes.toBytes(org_id));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("TS"),Bytes.toBytes(ts));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("AUTOMATEDKEYWORDSOPTIN"),Bytes.toBytes(automatedKeywordsOptIn));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("CPAGOAL_AMOUNT"),Bytes.toBytes(cpaGoal_Amount));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("DEFAULTCPCBID_AMOUNT"),Bytes.toBytes(defaultCpcBid_Amount));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("DELETED"),Bytes.toBytes(deleted));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("DISPLAYSTATUS"),Bytes.toBytes(displayStatus));

            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("ENDTIME"),Bytes.toBytes(endTime));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("MODIFICATIONTIME"),Bytes.toBytes(modificationTime));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("NAME"),Bytes.toBytes(name));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("SERVINGSTATEREASONS"),Bytes.toBytes(servingStateReasons));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("SERVINGSTATUS"),Bytes.toBytes(servingStatus));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("STARTTIME"),Bytes.toBytes(startTime));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("STATUS"),Bytes.toBytes(status));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("MINAGE"),Bytes.toBytes(minAge));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("MAXAGE"),Bytes.toBytes(maxAge));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("GENDER"),Bytes.toBytes(gender));

            //7
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("COUNTRY"),Bytes.toBytes(country));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("ADMINAREA"),Bytes.toBytes(adminArea));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("LOCALITY"),Bytes.toBytes(locality));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("DEVICECLASS"),Bytes.toBytes(deviceClass));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("DAYPART"),Bytes.toBytes(daypart));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("APPDOWNLOADERS_INCLUDED"),Bytes.toBytes(appDownloaders_included));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("APPDOWNLOADERS_EXCLUDED"),Bytes.toBytes(appDownloaders_excluded));


            try {
                meta_adgroup.put(put);
            } catch (IOException e) {
                e.printStackTrace();
            }

        }



    }

}
