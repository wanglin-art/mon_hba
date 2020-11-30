package org.monba.service.impl;

import com.mongodb.client.FindIterable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.bson.Document;
import org.json.JSONObject;
import org.monba.service.Report_keywordService;
import org.monba.service.Report_searchtermService;
import org.monba.utils.HbaseUtil;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;

@Service("report_searchtermService")
public class Report_searchtermServiceImpl implements Report_searchtermService {
    int count = 0;
    @Resource
    private MongoTemplate mongoTemplate;

    @Override
    public void getData() {

        Map<String, JSONObject> map = new HashMap<>();
        FindIterable<Document> documents = mongoTemplate.getCollection("report_searchterm").find();
        for(Document document : documents) {
            if(document == null) {
                continue;
            }
            JSONObject jsonObject = new JSONObject(document.toJson());
          //关于report_searchterm表的rowkey，因为不能根据各种东西拼接而得到唯一的key 所以到时候只能查二级索引。所以在此用UUID随机生成

            String row_key = UUID.randomUUID().toString();
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
        Table report_searchterm = null;
        //在这里获取hbase的连接
        Connection hbaseConnection = HbaseUtil.getHbaseConnection();
        try {
            report_searchterm = hbaseConnection.getTable(TableName.valueOf("REPORT_SEARCHTERM"));
        } catch (IOException e) {
            e.printStackTrace();
        }
            //-----------------------解析---------------------------------
            for (JSONObject jsonObject : list) {
                long ad_id = jsonObject.getLong("ad_id");  //列名为：ad_id
                String cert = jsonObject.getString("cert");  //列名为：cert
                long cp_id = jsonObject.getLong("cp_id");    //列名为：cp_id
                long kw_id;
                if(jsonObject.isNull("kw_id")){
                    kw_id=0L;
                }else{
                    kw_id = jsonObject.getLong("kw_id");    //列名为：kw_id
                }
                long org_id = jsonObject.getLong("org_id");  //列名为：org_id


                // term
                String term;  //列名为
                if(jsonObject.isNull("term")){
                    term="";
                }else{
                    term= jsonObject.getString("term");
                }


                long ts = jsonObject.getLong("ts");          //列名为：ts
                boolean adGroupDeleted = jsonObject.getBoolean("adGroupDeleted");//列名为：adGroupDeleted
                Long adGroupId = jsonObject.getLong("adGroupId");//列名为：adGroupId
                String adGroupName = jsonObject.getString("adGroupName");//列名为：adGroupName

                //avgCPA
                JSONObject avgCPA = jsonObject.getJSONObject("avgCPA");
                double  avgCPA_Amount = avgCPA.getDouble("amount");//列名为：avgCPA_Amount

                //avgCPT
                JSONObject avgCPT = jsonObject.getJSONObject("avgCPT");
                double avgCPT_Amount = avgCPT.getDouble("amount");//列名为：avgCPT_Amount

                //bidAmount_Amount
                JSONObject bidAmountJSON = jsonObject.getJSONObject("bidAmount");
                double bidAmount_Amount = bidAmountJSON.getDouble("amount");//列名为

                //conversionRate
                long conversionRate = jsonObject.getLong("conversionRate");//列名为

                //deleted  这里的deleted有可能为null，有可能为true或者false，因此设置为string类型
                String deleted;//列名为
                if(jsonObject.isNull("deleted")){
                    deleted ="";
                }else{
                    boolean a = jsonObject.getBoolean("deleted");
                    if (a){ //true
                        deleted ="true";
                    }else {
                        deleted ="false";
                    }
                }


                //impressions
                long impressions = jsonObject.getLong("impressions");//列名为

                //installs
                long installs = jsonObject.getLong("installs");//列名为

                //keyword
                String keyword;
                if (jsonObject.isNull("keyword")){
                    keyword="";
                }else{

                    keyword= jsonObject.getString("keyword");//列名为：
                }

                //keywordDisplayStatus
                String keywordDisplayStatus;
                if(jsonObject.isNull("keywordDisplayStatus")){
                    keywordDisplayStatus="";
                }else{
                    keywordDisplayStatus= jsonObject.getString("keywordDisplayStatus");//列名为：
                }

                //keywordId
                long keywordId;
                if(jsonObject.isNull("keywordId")){
                    keywordId=0L;
                }else{
                    keywordId= jsonObject.getLong("keywordId");//列名为：

                }


                //latOffInstalls
                long latOffInstalls = jsonObject.getLong("latOffInstalls");//列名为：

                //latOnInstalls
                long latOnInstalls = jsonObject.getLong("latOnInstalls");//列名为：

                //localSpend_Amount
                JSONObject localSpendJSON = jsonObject.getJSONObject("localSpend");
                double localSpend_Amount = localSpendJSON.getDouble("amount");//列名为：

                //matchType
                String matchType = jsonObject.getString("matchType"); //列名为

                //newDownloads
                long newDownloads = jsonObject.getLong("newDownloads");//列名为：

                //redownloads
                long redownloads = jsonObject.getLong("redownloads");//列名为：

                //searchTermSource
                String searchTermSource = jsonObject.getString("searchTermSource");//列名为

                //searchtermText
                String searchtermText;//列名为
                if(jsonObject.isNull("searchtermText")){
                    searchtermText="";
                }else{
                    searchtermText= jsonObject.getString("searchtermText");
                }

                //taps
                long taps = jsonObject.getLong("taps");//列名为

                //ttr
                long ttr = jsonObject.getLong("ttr");//列名

                //解析完毕 写入hbase
                //--------------------------------------------------------

            //设置rowKey

            String row_key = UUID.randomUUID().toString();
            Put put = new Put(Bytes.toBytes(row_key));
            //添加每一个列
                //添加每一个列31
                put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("AD_ID"),Bytes.toBytes(ad_id));
                put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("CERT"),Bytes.toBytes(cert));
                put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("CP_ID"),Bytes.toBytes(cp_id));
                put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("KW_ID"),Bytes.toBytes(kw_id));
                put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("ORG_ID"),Bytes.toBytes(org_id));
                put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("TERM"),Bytes.toBytes(term));
                put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("TS"),Bytes.toBytes(ts));
                put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("ADGROUPDELETED"),Bytes.toBytes(adGroupDeleted));
                put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("ADGROUPID"),Bytes.toBytes(adGroupId));
                put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("ADGROUPNAME"),Bytes.toBytes(adGroupName));

                put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("AVGCPA_AMOUNT"),Bytes.toBytes(avgCPA_Amount));
                put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("AVGCPT_AMOUNT"),Bytes.toBytes(avgCPT_Amount));
                put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("BIDAMOUNT_AMOUNT"),Bytes.toBytes(bidAmount_Amount));
                put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("CONVERSIONRATE"),Bytes.toBytes(conversionRate));
                put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("DELETED"),Bytes.toBytes(deleted));
                put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("IMPRESSIONS"),Bytes.toBytes(impressions));
                put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("INSTALLS"),Bytes.toBytes(installs));
                put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("KEYWORD"),Bytes.toBytes(keyword));
                put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("KEYWORDDISPLAYSTATUS"),Bytes.toBytes(keywordDisplayStatus));
                put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("KEYWORDID"),Bytes.toBytes(keywordId));

                put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("LATOFFINSTALLS"),Bytes.toBytes(latOffInstalls));
                put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("LATONINSTALLS"),Bytes.toBytes(latOnInstalls));
                put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("LOCALSPEND_AMOUNT"),Bytes.toBytes(localSpend_Amount));
                put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("MATCHTYPE"),Bytes.toBytes(matchType));
                put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("NEWDOWNLOADS"),Bytes.toBytes(newDownloads));
                put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("REDOWNLOADS"),Bytes.toBytes(redownloads));
                put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("SEARCHTERMSOURCE"),Bytes.toBytes(searchTermSource));
                put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("SEARCHTERMTEXT"),Bytes.toBytes(searchtermText));
                put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("TAPS"),Bytes.toBytes(taps));
                put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("TTR"),Bytes.toBytes(ttr));
            //TODO
            try {
                report_searchterm.put(put);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        HbaseUtil.close(hbaseConnection);
    }

}
