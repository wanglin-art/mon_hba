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
import org.monba.service.NegativeService;
import org.monba.utils.HbaseUtil;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service("negativeService")
public class NegativeServiceImpl implements NegativeService {
int count =0;

    @Resource
    private MongoTemplate mongoTemplate;

    @Override
    public void getData() {
        Map<Long, JSONObject> map = new HashMap<>();
        FindIterable<Document> documents = mongoTemplate.getCollection("meta_negative").find();
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
            transmit(new ArrayList<JSONObject>(map.values()));
        }
    }

    private void transmit(List<JSONObject> list) {
        count++;
        System.out.println("正在处理第："+count+"万条数据");
        Table meta_negative = null;
        //在这里获取hbase的连接
        Connection hbaseConnection = HbaseUtil.getHbaseConnection();
        try {
            meta_negative = hbaseConnection.getTable(TableName.valueOf("META_NEGATIVE"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (JSONObject jsonObject : list) {
            String cert = jsonObject.getString("cert");  //列名为：cert
            long cp_id = jsonObject.getLong("cp_id");    //列名为：cp_id
            long kw_id = jsonObject.getLong("kw_id");    //列名为：kw_id
            long org_id = jsonObject.getLong("org_id");  //列名为：org_id
            long ts = jsonObject.getLong("ts");          //列名为：ts
            String adGroupId = "";
            long campaignId = jsonObject.getLong("campaignId");
            boolean deleted = jsonObject.getBoolean("deleted");
            String matchType = jsonObject.getString("matchType");
            String modificationTime = jsonObject.getString("modificationTime");
            String status = jsonObject.getString("status");
            String text = jsonObject.getString("text");


            //设置rowKey
            Put put = new Put(Bytes.toBytes(cp_id));
            //添加每一个列
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("cert"),Bytes.toBytes(cert));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("cp_id"),Bytes.toBytes(cp_id));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("kw_id"),Bytes.toBytes(kw_id));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("org_id"),Bytes.toBytes(org_id));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("ts"),Bytes.toBytes(ts));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("adGroupId"),Bytes.toBytes(adGroupId));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("campaignId"),Bytes.toBytes(campaignId));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("deleted"),Bytes.toBytes(deleted));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("matchType"),Bytes.toBytes(matchType));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("modificationTime"),Bytes.toBytes(modificationTime));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("status"),Bytes.toBytes(status));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("text"),Bytes.toBytes(text));

            try {
                meta_negative.put(put);
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        HbaseUtil.close(hbaseConnection);
    }
}
