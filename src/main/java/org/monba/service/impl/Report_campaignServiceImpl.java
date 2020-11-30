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
import org.monba.service.Report_campaignService;
import org.monba.utils.HbaseUtil;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service("report_campaignService")
public class Report_campaignServiceImpl implements Report_campaignService {
    int count = 0;
    @Resource
    private MongoTemplate mongoTemplate;


    @Override
    public void getData() {
        Map<String, JSONObject> map = new HashMap<>();
        FindIterable<Document> documents = mongoTemplate.getCollection("report_campaign").find();
        for(Document document : documents) {
            if(document == null) {
                continue;
            }
            JSONObject jsonObject = new JSONObject(document.toJson());
            Long cp_id = jsonObject.getLong("cp_id");
            Long ts = jsonObject.getLong("ts");
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            String time = simpleDateFormat.format(ts*1000L);
            String row_key = cp_id +"_"+ time;

            map.put(row_key, jsonObject);
            if(map.keySet().size() >= 10000) {
                transmit(new ArrayList<JSONObject>(map.values()));
                map.clear();
            }
        }

        if(!map.isEmpty()) {
            List<JSONObject> list = new ArrayList<>();
            for(JSONObject jsonObject : map.values()) {
                list.add(jsonObject);
                if(list.size() >= 1000) {
                    List<JSONObject> temp = new ArrayList<>(list);
                    Thread thread = new Thread() {
                        @Override
                        public void run() {
                            transmit(temp);
                        }
                    };
                    thread.start();
                    list.clear();
                }
            }
            if(!list.isEmpty()) {
                transmit(list);
            }
        }

    }

    private void transmit(List<JSONObject> list) {
        count++;
        System.out.println("正在处理第："+count+"万条数据");
        Table report_campaign = null;
        //在这里获取hbase的连接
        Connection hbaseConnection = HbaseUtil.getHbaseConnection();
        try {
            report_campaign = hbaseConnection.getTable(TableName.valueOf("REPORT_CAMPAIGN"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (JSONObject jsonObject : list) {
            String cert = jsonObject.getString("cert");  //列名为：cert
            long cp_id = jsonObject.getLong("cp_id");    //列名为：cp_id
            long org_id = jsonObject.getLong("org_id");  //列名为：org_id
            long ts = jsonObject.getLong("ts");          //列名为：ts
            String adChannelType = jsonObject.getString("adChannelType");//列名为：adChannelType

            //app
            JSONObject app = jsonObject.getJSONObject("app");
            String appName = app.getString("appName");//列名为：
            long adamId = app.getLong("adamId");    //列名为：

            //avgCPA
            JSONObject avgCPA = jsonObject.getJSONObject("avgCPA");
            double avgCPA_Amount = avgCPA.getDouble("amount");//列名为：

            //avgCPT
            JSONObject avgCPT = jsonObject.getJSONObject("avgCPT");
            double avgCPT_Amount = avgCPT.getDouble("amount");//列名为：

            //需要去更改
            double campaignId = jsonObject.getDouble("campaignId");//列名为：
            String campaignName = jsonObject.getString("campaignName");//列名为：
            String campaignStatus = jsonObject.getString("campaignStatus");//列名为：
            double conversionRate = jsonObject.getDouble("conversionRate");//列名为：

            //countriesOrRegions
            JSONArray countriesOrRegionsJSON = jsonObject.getJSONArray("countriesOrRegions");
            // for循环拼接
            JSONArray jsonArrayX = new JSONArray();
            for (Object o : countriesOrRegionsJSON) {
                jsonArrayX.put(o);
            }
            String  countriesOrRegions= jsonArrayX.toString(); //列名为：countriesOrRegions

            //countryOrRegionServingStateReasons 一直都是空值
            String countryOrRegionServingStateReasons ="";//列名为：

            //dailyBudget 需要判空
            double dailyBudget_Amount;
            if(jsonObject.isNull("dailyBudget")){
                dailyBudget_Amount=0.0;
            }else{
                JSONObject dailyBudgetJSON = jsonObject.getJSONObject("dailyBudget");
                dailyBudget_Amount = dailyBudgetJSON.getDouble("amount");//列名为：
            }


            //deleted
            boolean deleted = jsonObject.getBoolean("deleted");//列名为：

            //displayStatus
            String displayStatus = jsonObject.getString("displayStatus");//列名为：

            //impressions
            long impressions = jsonObject.getLong("impressions");//列名为：

            //installs
            long installs = jsonObject.getLong("installs");//列名为：

            //latOffInstalls
            long latOffInstalls = jsonObject.getLong("latOffInstalls");//列名为：

            //latOnInstalls
            long latOnInstalls = jsonObject.getLong("latOnInstalls");//列名为：

            //localSpend_Amount
            JSONObject localSpendJSON = jsonObject.getJSONObject("localSpend");
            double localSpend_Amount = localSpendJSON.getDouble("amount");//列名为：

            //modificationTime
            String modificationTime = jsonObject.getString("modificationTime");//列名为：

            //newDownloads
            long newDownloads = jsonObject.getLong("newDownloads");//列名为：

            //orgId
            long orgId = jsonObject.getLong("orgId");//列名为：

            //redownloads
            long redownloads = jsonObject.getLong("redownloads");//列名为：

            //servingStateReasons  拼接
            String  servingStateReasons;
            if(jsonObject.isNull("servingStateReasons")){
                servingStateReasons="";
            }else{
                // for循环拼接
                JSONArray servingStateReasonsJSON = jsonObject.getJSONArray("servingStateReasons");
                JSONArray jsonArrayZ = new JSONArray();
                for (Object o : servingStateReasonsJSON) {
                    jsonArrayZ.put(o);
                }
                servingStateReasons = jsonArrayZ.toString();//列名为：
            }

            //servingStatus
            String servingStatus = jsonObject.getString("servingStatus");//列名为：

            //supplySources
            JSONArray supplySourcesJSON = jsonObject.getJSONArray("supplySources");
            JSONArray jsonArrayA = new JSONArray();
            for (Object o : supplySourcesJSON) {
                jsonArrayA.put(o);
            }
            String supplySources = jsonArrayA.toString();//列名为：

            //taps
            long taps = jsonObject.getLong("taps");//列名为：

            //totalBudget_Amount
            JSONObject totalBudget = jsonObject.getJSONObject("totalBudget");
            double totalBudget_Amount = totalBudget.getDouble("amount");//列名

            //ttr
            long ttr = jsonObject.getLong("ttr");//列名

            //设置rowKey
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            String time = simpleDateFormat.format(ts*1000L);
            String row_key = cp_id +"_"+ time;

            Put put = new Put(Bytes.toBytes(row_key));
            //添加每一个列
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("CERT"),Bytes.toBytes(cert));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("CP_ID"),Bytes.toBytes(cp_id));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("ORG_ID"),Bytes.toBytes(org_id));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("TS"),Bytes.toBytes(ts));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("ADCHANNELTYPE"),Bytes.toBytes(adChannelType));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("APPNAME"),Bytes.toBytes(appName));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("ADAMID"),Bytes.toBytes(adamId));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("AVGCPA_AMOUNT"),Bytes.toBytes(avgCPA_Amount));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("AVGCPT_AMOUNT"),Bytes.toBytes(avgCPT_Amount));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("CAMPAIGNID"),Bytes.toBytes(campaignId));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("CAMPAIGNNAME"),Bytes.toBytes(campaignName));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("CAMPAIGNSTATUS"),Bytes.toBytes(campaignStatus));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("CONVERSIONRATE"),Bytes.toBytes(conversionRate));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("COUNTRIESORREGIONS"),Bytes.toBytes(countriesOrRegions));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("COUNTRYORREGIONSERVINGSTATEREASONS"),Bytes.toBytes(countryOrRegionServingStateReasons));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("DAILYBUDGET_AMOUNT"),Bytes.toBytes(dailyBudget_Amount));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("DELETED"),Bytes.toBytes(deleted));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("DISPLAYSTATUS"),Bytes.toBytes(displayStatus));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("IMPRESSIONS"),Bytes.toBytes(impressions));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("INSTALLS"),Bytes.toBytes(installs));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("LATOFFINSTALLS"),Bytes.toBytes(latOffInstalls));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("LATONINSTALLS"),Bytes.toBytes(latOnInstalls));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("LOCALSPEND_AMOUNT"),Bytes.toBytes(localSpend_Amount));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("MODIFICATIONTIME"),Bytes.toBytes(modificationTime));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("NEWDOWNLOADS"),Bytes.toBytes(newDownloads));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("ORGID"),Bytes.toBytes(orgId));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("REDOWNLOADS"),Bytes.toBytes(redownloads));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("SERVINGSTATEREASONS"),Bytes.toBytes(servingStateReasons));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("SERVINGSTATUS"),Bytes.toBytes(servingStatus));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("SUPPLYSOURCES"),Bytes.toBytes(supplySources));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("TAPS"),Bytes.toBytes(taps));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("TOTALBUDGET_AMOUNT"),Bytes.toBytes(totalBudget_Amount));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("TTR"),Bytes.toBytes(ttr));
            //TODO
            try {
                report_campaign.put(put);
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        HbaseUtil.close(hbaseConnection);
    }

}
