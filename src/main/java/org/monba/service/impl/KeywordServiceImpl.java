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
import org.monba.service.KeywordService;
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

@Service("keywordService")
public class KeywordServiceImpl implements KeywordService {
    int count = 0;
    @Resource
    private MongoTemplate mongoTemplate;

    @Override
    public void getData() {
        Map<Long, JSONObject> map = new HashMap<>();
        FindIterable<Document> documents = mongoTemplate.getCollection("meta_keyword").find();
        for(Document document : documents) {
            if(document == null) {
                continue;
            }
            JSONObject jsonObject = new JSONObject(document.toJson());
            Long kw_id = jsonObject.getLong("kw_id");
            map.put(kw_id, jsonObject);
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
        Table meta_keyword = null;
        //在这里获取hbase的连接
        Connection hbaseConnection = HbaseUtil.getHbaseConnection();
        try {
            meta_keyword = hbaseConnection.getTable(TableName.valueOf("META_KEYWORD"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (JSONObject jsonObject : list) {
            long ad_id = jsonObject.getLong("ad_id");  //列名为：ad_id
            String cert = jsonObject.getString("cert");  //列名为：cert
            long cp_id = jsonObject.getLong("cp_id");    //列名为：cp_id
            long kw_id = jsonObject.getLong("kw_id");    //列名为：kw_id
            long org_id = jsonObject.getLong("org_id");  //列名为：org_id
            long ts = jsonObject.getLong("ts");          //列名为：ts

            JSONObject bidAmountJSON = jsonObject.getJSONObject("bidAmount");
            double bid_Amount = bidAmountJSON.getDouble("amount");              //列名为：bid_Amount

            boolean deleted = jsonObject.getBoolean("deleted");         //列名为：deleted
            String matchType = jsonObject.getString("matchType");       //列名为：matchType
            String modificationTime = jsonObject.getString("modificationTime"); //列名为：modificationTime
            String status = jsonObject.getString("status");             //列名为：status
            String text = jsonObject.getString("text");                 //列名为：text

            //hbase相关
            //设置rowKey
            Put put = new Put(Bytes.toBytes(kw_id));
            //添加每一个列
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("AD_ID"),Bytes.toBytes(ad_id));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("CERT"),Bytes.toBytes(cert));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("CP_ID"),Bytes.toBytes(cp_id));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("KW_ID"),Bytes.toBytes(kw_id));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("ORG_ID"),Bytes.toBytes(org_id));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("TS"),Bytes.toBytes(ts));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("BID_AMOUNT"),Bytes.toBytes(bid_Amount));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("DELETED"),Bytes.toBytes(deleted));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("MATCHTYPE"),Bytes.toBytes(matchType));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("MODIFICATIONTIME"),Bytes.toBytes(modificationTime));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("STATUS"),Bytes.toBytes(status));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("TEXT"),Bytes.toBytes(text));
            try {
                meta_keyword.put(put);
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        HbaseUtil.close(hbaseConnection);
    }
}
