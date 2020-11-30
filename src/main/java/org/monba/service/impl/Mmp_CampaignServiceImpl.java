package org.monba.service.impl;

import com.mongodb.client.FindIterable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.IFile;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;
import org.monba.service.Mmp_CampaignService;
import org.monba.service.Report_searchtermService;
import org.monba.utils.HbaseUtil;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.*;

@Service("mmp_CampaignService")
public class Mmp_CampaignServiceImpl implements Mmp_CampaignService {
    int count = 0;
    @Resource
    private MongoTemplate mongoTemplate;

    @Override
    public void getData() {
        Map<String, JSONObject> map = new HashMap<>();
        FindIterable<Document> documents = mongoTemplate.getCollection("mmp_campaign").find();
        for (Document document : documents) {
            if (document == null) {
                continue;
            }
            JSONObject jsonObject = new JSONObject(document.toJson());
            //这里用了optLong.因为cp_id有可能是int32也有可能是int64
            Long cp_id = jsonObject.optLong("cp_id");
            Long ts = jsonObject.getLong("ts");

            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            String time = simpleDateFormat.format(ts * 1000L);
            String row_key = cp_id + "_" + time;

            map.put(row_key, jsonObject);
            if (map.keySet().size() >= 10000) {
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


        //-----------------------解析---------------------------------
        for (JSONObject jsonObject : list) {
            long appid = jsonObject.getLong("appid"); //列名为
            long cp_id = jsonObject.optLong("cp_id");    //列名为：cp_id
            long ts = jsonObject.getLong("ts");          //列名为：ts




            String source = jsonObject.getString("source");//列名为

            //设置rowKey
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            String time = simpleDateFormat.format(ts * 1000L);
            String row_key = cp_id + "_" + time;
            Put put = new Put(Bytes.toBytes(row_key));


            //因为currency的值有可能没有。因此判断如果有这个值就加。
            if (jsonObject.has("currency")){
                String currency = jsonObject.getString("currency"); //列名为
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("CURRENCY"), Bytes.toBytes(currency));
            }

            //逻辑
            //mmp_campaign是在原来的表中多添加几个列名
            //其中 data字段下可能有多个json对象（超过30个） 有几个就加几个列
            //每一个json对象中 都有三个键值对 kpi name value
            //每一个json对象  都只在要添加的那个行中多加入一个列 列名为name的值（依照逻辑有可能会变化）  值为value的值
            //判断如果kpi对应的字符串是以"_revenue"结尾的 那么该列名变为name的值+"_revenue"
            //判断如果kpi对应的字符串为install 那么该列名变为"actives"
            //其他的都按照默认
            JSONArray dataJSONArray = jsonObject.getJSONArray("data");
            for (int i = 0; i < dataJSONArray.length(); i++) {
                JSONObject dataObject = dataJSONArray.getJSONObject(i);
                String kpi = dataObject.getString("kpi");
                double value ;
                if (dataObject.isNull("value")){
                    value=0.0;
                }else{
                    value = dataObject.getDouble("value");
                }
                String name = dataObject.getString("name");
                String[] s = kpi.split("_");

                if (name.equals("installs")) {
                    //添加列
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("INSTALLS"), Bytes.toBytes(value));
                } else if (s.length == 2 && s[1].equals("_revenue")) {
                    String qualifier = name + "_" + s[1];
                    //添加列
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(qualifier.toUpperCase()), Bytes.toBytes(value));
                } else {
                    //添加列
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(name.toUpperCase()), Bytes.toBytes(value));
                }
            }
            //添加列
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("APPID"), Bytes.toBytes(appid));

            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("SOURCE"), Bytes.toBytes(source));

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
