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
import org.monba.service.Mmp_adgroupService;
import org.monba.service.Mmp_keywordService;
import org.monba.utils.HbaseUtil;
import org.monba.utils.PhoenixUtil;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

@Service("mmp_keywordService")
public class Mmp_keywordServiceImpl implements Mmp_keywordService {
    int count = 0;
    int phoenixSelect = 0;
    @Resource
    private MongoTemplate mongoTemplate;

    @Override
    public void getData() {
        Map<String, JSONObject> map = new HashMap<>();
        FindIterable<Document> documents = mongoTemplate.getCollection("mmp_keyword").find();
        for (Document document : documents) {
            if (document == null) {
                continue;
            }
            JSONObject jsonObject = new JSONObject(document.toJson());

                Long ad_id = jsonObject.getLong("ad_id");
            UUID uuid = UUID.randomUUID();
            String row_key = uuid +"_"+ad_id;
            map.put(row_key, jsonObject);



            if (map.keySet().size() >= 10000) {
                transmit(new ArrayList<JSONObject>(map.values()));
                map.clear();
            }
        }
        if (!map.isEmpty()) {
            List<JSONObject> list = new ArrayList<>();
            for (JSONObject jsonObject : map.values()) {
                list.add(jsonObject);
                if (list.size() >= 1000) {
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
            if (!list.isEmpty()) {
                transmit(list);
            }
        }
    }


    private void transmit(List<JSONObject> list) {
        count++;
        System.out.println("正在处理第：" + count + "万条数据");
        Table report_adgroup = null;
        //在这里获取hbase的连接
        Connection hbaseConnection = HbaseUtil.getHbaseConnection();
        try {
            report_adgroup = hbaseConnection.getTable(TableName.valueOf("REPORT_KEYWORD"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        //获取phoenix的链接
        java.sql.Connection phoenixConnection=null;
        try {
            phoenixConnection= PhoenixUtil.getConnection();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }


        //-----------------------解析---------------------------------
        for (JSONObject jsonObject : list) {
            long appid = jsonObject.getLong("appid"); //列名为
            long ts = jsonObject.getLong("ts");          //列名为：ts
            String source = jsonObject.getString("source");//列名为
            Put put = null;
            //开始获得判断-------------------------kw_id-------------------
            if (!jsonObject.isNull("kw_id")){
                //设置rowKey
                long kw_id = jsonObject.getLong("kw_id");
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                String time = simpleDateFormat.format(ts * 1000L);
                String row_key = kw_id + "_" + time;
                put = new Put(Bytes.toBytes(row_key));
                //因为currency的值有可能没有。因此判断如果有这个值就加。
                if (jsonObject.has("currency")) {
                    String currency = jsonObject.getString("currency"); //列名为
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("CURRENCY"), Bytes.toBytes(currency));
                }
                //逻辑
                //mmp_adgroup是在原来的表中多添加几个列名
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

                    long value;
                    if (dataObject.isNull("value")) {
                        value = 0;
                    } else {
                        value = dataObject.getLong("value");
                    }

                    String name = dataObject.getString("name");
                    String[] s = kpi.split("_");

                    if (name.equals("installs")) {
                        //添加列
                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("ACTIVES"), Bytes.toBytes(value));
                    } else if (s.length == 2 && s[1].equals("revenue")) {
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
                    report_adgroup.put(put);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }else if (jsonObject.isNull("kw_id") && source.equals("appsflyer")){//就去phoenix中的meta_keyword表中查询
                phoenixSelect++;
                System.out.println("到Phoenix中查询了"+phoenixSelect);
                //如果为空那么kw_id为null 则查出ad_id和kw 然后将这两个条件拿到phoenix中查询得到rowkey
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                String time = simpleDateFormat.format(ts * 1000L);
                long ad_id = jsonObject.getLong("ad_id");
                String kw = jsonObject.getString("kw");
                //这里还需要注意Hbase是区分大小写的，
                // Phoenix 默认会把sql语句中的小写转换成大写，再建表，
                // 如果不希望转换，需要将表名，字段名等使用引号。
                //  但是查询条件的值不用
//                String s = "select kw_id from meta_keyword where ad_id = %d and kw = %d";
//                String sql = String.format(s, ad_id, kw);
//                String sql = "select kw_id from meta_keyword where ad_id = "+ad_id+" and text = " +"\'kw+\'";
                String sql = "select kw_id from meta_keyword where ad_id = "+ad_id+" and text = " +"\'"+kw+"\'";
                //拿到的是kw_id的集合 因此遍历然后将数据写入
                    //111
                //在这里获取phoenix的连接
                PreparedStatement preparedStatement;
                try {
                    preparedStatement= phoenixConnection.prepareStatement(sql);
                    ResultSet resultSet = preparedStatement.executeQuery();

                    while (resultSet.next()) {
                        long aLong = resultSet.getLong(1);
                        String row_key = aLong+"_" + time;
                        System.out.println(row_key);
                        put = new Put(Bytes.toBytes(row_key));
                        //因为currency的值有可能没有。因此判断如果有这个值就加。
                        if (jsonObject.has("currency")) {
                            String currency = jsonObject.getString("currency"); //列名为
                            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("CURRENCY"), Bytes.toBytes(currency));
                        }
                        //逻辑
                        //mmp_adgroup是在原来的表中多添加几个列名
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

                            double value;
                            if (dataObject.isNull("value")) {
                                value = 0.0;
                            } else {
                                value = dataObject.getDouble("value");
                            }

                            String name = dataObject.getString("name");
                            String[] s = kpi.split("_");

                            if (name.equals("installs")) {
                                //添加列

                                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("ACTIVES"), Bytes.toBytes(value));

                            } else if (s.length == 2 && s[1].equals("revenue")) {
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
                            report_adgroup.put(put);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }




            }


        }
        HbaseUtil.close(hbaseConnection);
        PhoenixUtil.close(phoenixConnection);
    }

}
