package org.monba.service.impl;

import com.mongodb.client.FindIterable;
import com.sun.org.apache.bcel.internal.generic.IF_ACMPEQ;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;
import org.monba.service.Report_adgroupService;
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

@Service("report_adgroupService")
public class Report_adgroupServiceImpl implements Report_adgroupService {
    int count = 0;
    @Resource
    private MongoTemplate mongoTemplate;

    @Override
    public void getData() {
        Map<String, JSONObject> map = new HashMap<>();
        FindIterable<Document> documents = mongoTemplate.getCollection("report_adgroup").find();
        for(Document document : documents) {
            if(document == null) {
                continue;
            }
            JSONObject jsonObject = new JSONObject(document.toJson());
            Long ad_id = jsonObject.getLong("ad_id");
            Long ts = jsonObject.getLong("ts");
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            String time = simpleDateFormat.format(ts*1000L);
            String row_key = ad_id +"_"+ time;

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
        Table report_adgroup = null;
        //在这里获取hbase的连接
        Connection hbaseConnection = HbaseUtil.getHbaseConnection();
        try {
            report_adgroup = hbaseConnection.getTable(TableName.valueOf("REPORT_ADGROUP"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        //-----------------------解析---------------------------------
        for (JSONObject jsonObject : list) {
            long ad_id = jsonObject.getLong("ad_id");  //列名为：ad_id
            String cert = jsonObject.getString("cert");  //列名为：cert
            long cp_id = jsonObject.getLong("cp_id");    //列名为：cp_id
            long org_id = jsonObject.getLong("org_id");  //列名为：org_id
            long ts = jsonObject.getLong("ts");          //列名为：ts
            String adGroupDisplayStatus = jsonObject.getString("adGroupDisplayStatus");//列名为：adGroupDisplayStatus
            long adGroupId = jsonObject.getLong("adGroupId"); //列名为：
            String adGroupName = jsonObject.getString("adGroupName");//列名为：

            //adGroupServingStateReasons 有可能为空 因此需要判断
            String  adGroupServingStateReasons;
            if(jsonObject.isNull("adGroupServingStateReasons")){
                adGroupServingStateReasons="";
            }else{
                // for循环拼接
                JSONArray adGroupServingStateReasonsJSON = jsonObject.getJSONArray("adGroupServingStateReasons");
                JSONArray jsonArrayX = new JSONArray();
                for (Object o : adGroupServingStateReasonsJSON) {
                    jsonArrayX.put(o);
                }
                adGroupServingStateReasons= jsonArrayX.toString(); //列名为：
            }


            //adGroupServingStatus
            String adGroupServingStatus = jsonObject.getString("adGroupServingStatus");//列名为：
            //adGroupStatus
            String adGroupStatus = jsonObject.getString("adGroupStatus");//列名为：
            //automatedKeywordsOptIn
            boolean automatedKeywordsOptIn = jsonObject.getBoolean("automatedKeywordsOptIn"); //列名为：

            //avgCPA
            JSONObject avgCPA = jsonObject.getJSONObject("avgCPA");
            double avgCPA_Amount = avgCPA.getDouble("amount");//列名为：avgCPA_Amount

            //avgCPT
            JSONObject avgCPT = jsonObject.getJSONObject("avgCPT");
            double avgCPT_Amount = avgCPT.getDouble("amount");//列名为：avgCPT_Amount

            //campaignId
            long campaignId = jsonObject.getLong("campaignId");//列名为
            //conversionRate
            long conversionRate = jsonObject.getLong("conversionRate");//列名为

            //cpaGoal 需要判空
            double cpaGoal_Amount;
            if (jsonObject.isNull("cpaGoal")){
                cpaGoal_Amount = 0.0;
            }else{
                JSONObject cpaGoal = jsonObject.getJSONObject("cpaGoal");
                cpaGoal_Amount= cpaGoal.getDouble("amount");//列名为：cpaGoal_Amount
            }


            //defaultCpcBid
            JSONObject defaultCpcBid = jsonObject.getJSONObject("defaultCpcBid");
            double defaultCpcBid_Amount = defaultCpcBid.getDouble("amount");//列名为：cpaGoal_Amount

            //deleted
            boolean deleted = jsonObject.getBoolean("deleted");//列名为

            //endTime 一直为null 不知道它的具体类型 因此存null
            String endTime ="";//列名为

            //impressions
            long impressions = jsonObject.getLong("impressions");//列名为

            //installs
            long installs = jsonObject.getLong("installs");//列名为

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

            //startTime
            String startTime = jsonObject.getString("startTime");//列名为

            //taps
            long taps = jsonObject.getLong("taps");//列名为

            //ttr
            long ttr = jsonObject.getLong("ttr");//列名
            //--------------------------------------------------------

            //--------------------存入hbase-----------------------------------
            //设置rowKey
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            String time = simpleDateFormat.format(ts*1000L);
            String row_key = ad_id +"_"+ time;
            Put put = new Put(Bytes.toBytes(row_key));
            //
            //添加每一个列32
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("AD_ID"),Bytes.toBytes(ad_id));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("CERT"),Bytes.toBytes(cert));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("CP_ID"),Bytes.toBytes(cp_id));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("ORG_ID"),Bytes.toBytes(org_id));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("TS"),Bytes.toBytes(ts));

            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("ADGROUPDISPLAYSTATUS"),Bytes.toBytes(adGroupDisplayStatus));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("ADGROUPID"),Bytes.toBytes(adGroupId));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("ADGROUPNAME"),Bytes.toBytes(adGroupName));

            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("ADGROUPSERVINGSTATEREASONS"),Bytes.toBytes(adGroupServingStateReasons));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("ADGROUPSERVINGSTATUS"),Bytes.toBytes(adGroupServingStatus));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("ADGROUPSTATUS"),Bytes.toBytes(adGroupStatus));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("AUTOMATEDKEYWORDSOPTIN"),Bytes.toBytes(automatedKeywordsOptIn));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("AVGCPA_AMOUNT"),Bytes.toBytes(avgCPA_Amount));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("AVGCPT_AMOUNT"),Bytes.toBytes(avgCPT_Amount));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("CAMPAIGNID"),Bytes.toBytes(campaignId));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("CONVERSIONRATE"),Bytes.toBytes(conversionRate));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("CPAGOAL_AMOUNT"),Bytes.toBytes(cpaGoal_Amount));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("DEFAULTCPCBID_AMOUNT"),Bytes.toBytes(defaultCpcBid_Amount));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("DELETED"),Bytes.toBytes(deleted));


            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("ENDTIME"),Bytes.toBytes(endTime));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("IMPRESSIONS"),Bytes.toBytes(impressions));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("INSTALLS"),Bytes.toBytes(installs));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("LATOFFINSTALLS"),Bytes.toBytes(latOffInstalls));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("LATONINSTALLS"),Bytes.toBytes(latOnInstalls));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("LOCALSPEND_AMOUNT"),Bytes.toBytes(localSpend_Amount));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("MODIFICATIONTIME"),Bytes.toBytes(modificationTime));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("NEWDOWNLOADS"),Bytes.toBytes(newDownloads));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("ORGID"),Bytes.toBytes(orgId));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("REDOWNLOADS"),Bytes.toBytes(redownloads));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("STARTTIME"),Bytes.toBytes(startTime));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("TAPS"),Bytes.toBytes(taps));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("TTR"),Bytes.toBytes(ttr));

            try {
                report_adgroup.put(put);
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        HbaseUtil.close(hbaseConnection);
    }

}
