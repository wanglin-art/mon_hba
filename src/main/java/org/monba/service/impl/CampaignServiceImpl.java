package org.monba.service.impl;

import com.mongodb.client.FindIterable;
import org.apache.avro.data.Json;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.bson.Document;
import org.eclipse.jetty.util.ajax.JSON;
import org.json.JSONArray;
import org.json.JSONObject;
import org.monba.service.CampaignService;
import org.monba.utils.HbaseUtil;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;
import sun.net.www.ParseUtil;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.*;

@Service("campaignService")
public class CampaignServiceImpl implements CampaignService {
    int count = 0;
    @Resource
    private MongoTemplate mongoTemplate;

    @Override
    public void getData() {
        Map<Long, JSONObject> map = new HashMap<>();
        FindIterable<Document> documents = mongoTemplate.getCollection("meta_campaign").find();
        for (Document document : documents) {
            if (document == null) {
                continue;
            }
            JSONObject jsonObject = new JSONObject(document.toJson());
            Long cp_id = jsonObject.getLong("cp_id");
            map.put(cp_id, jsonObject);
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
        Table meta_campaign = null;
        //在这里获取hbase的连接
        Connection hbaseConnection = HbaseUtil.getHbaseConnection();
        try {
            meta_campaign = hbaseConnection.getTable(TableName.valueOf("META_CAMPAIGN"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        List<Put> puts = new ArrayList<>();

        for (JSONObject jsonObject : list) {
            String cert = jsonObject.getString("cert");  //列名为：cert
            long cp_id = jsonObject.getLong("cp_id");    //列名为：cp_id
            long org_id = jsonObject.getLong("org_id");  //列名为：org_id
            long ts = jsonObject.getLong("ts");          //列名为：ts
            String adChannelType = jsonObject.getString("adChannelType"); //列名为：adChannelType
            long adamId = jsonObject.getLong("adamId");     //列名为：adamId

            JSONObject budgetAmountJSON = jsonObject.getJSONObject("budgetAmount");  //子json

            //11.23修改 测试环境 列名为：budgetAmount 值为 amount对应的值
            String budgetAmount = budgetAmountJSON.getString("amount");    //列名为：budgetAmount

            String budgetOrders = ""; //列名为：budgetOrders       子json

            JSONArray countriesOrRegionsString = jsonObject.getJSONArray("countriesOrRegions");
            JSONArray jsonArray = new JSONArray();
            for (Object o : countriesOrRegionsString) {
                jsonArray.put(o);
            }
            String countriesOrRegions = jsonArray.toString(); //列名为：countriesOrRegions

            //列名为：countryOrRegionServingStateReasons
            //countryOrRegionServingStateReasons是一个Object但是里面永没值  经过讨论 这个值设置为null
            String countryOrRegionServingStateReasons = "";

            //dailyBudgetAmount有可能为空有可能不为空
            //如果为空 直接赋值为null 如果不为空 获取第一个元素并赋值
            boolean dailyBudgetAmountIsNull = jsonObject.isNull("dailyBudgetAmount");
            String dailyBudgetAmount;
            if (dailyBudgetAmountIsNull) {
                dailyBudgetAmount = "";    //列名为：dailyBudgetAmount
            } else {
                JSONObject dailyBudgetAmount1 = jsonObject.getJSONObject("dailyBudgetAmount");
                dailyBudgetAmount = dailyBudgetAmount1.getString("amount");
            }

            Boolean deleted = jsonObject.getBoolean("deleted");    //列名为：deleted

            String displayStatus = jsonObject.getString("displayStatus"); //列名为：displayStatus

            boolean endTimeIsNull = jsonObject.isNull("endTime");
            String endTime;     //列名为：endTime
            if (endTimeIsNull) {
                endTime = "";    //列名为：dailyBudgetAmount
            } else {
                endTime = jsonObject.getString("endTime");
            }


            //逻辑：locInvoiceDetails有可能为null 有可能不为null 如果是null 则其名下无值 如果不为null 则名下有五个json对象（都是String类型的）
            //      如果是null值 依然有这五个列 但是值为null 如果不为null 则属性有值
            boolean locInvoiceDetailsIsNull = jsonObject.isNull("locInvoiceDetails");
            String clientName;     //列名为：endTime
            String orderNumber;     //列名为：endTime
            String buyerName;     //列名为：endTime
            String buyerEmail;     //列名为：endTime
            String billingContactEmail;     //列名为：endTime
            if (locInvoiceDetailsIsNull) {
                clientName = "";
                orderNumber = "";
                buyerName = "";
                buyerEmail = "";
                billingContactEmail = "";
            } else {
                JSONObject locInvoiceDetails = jsonObject.getJSONObject("locInvoiceDetails");
                clientName = locInvoiceDetails.getString("clientName");
                orderNumber = locInvoiceDetails.getString("orderNumber");
                buyerName = locInvoiceDetails.getString("buyerName");
                buyerEmail = locInvoiceDetails.getString("buyerEmail");
                billingContactEmail = locInvoiceDetails.getString("billingContactEmail");
            }


            String modificationTime = jsonObject.getString("modificationTime"); //列名为：modificationTime
            String name = jsonObject.getString("name");                         //列名为：name
            String paymentModel = jsonObject.getString("paymentModel");         //列名为：paymentModel
            String sapinLawResponse = jsonObject.getString("sapinLawResponse"); //列名为：sapinLawResponse

            // 列名为：servingStateReasons 的计算逻辑
            // servingStateReasons  如果不为null则为一个数组进行拼接 因此进行非空判断并拼接。
            boolean servingStateReasonsIsNull = jsonObject.isNull("servingStateReasons");
            String servingStateReasons;     //列名为：endTime
            if (servingStateReasonsIsNull) {
                servingStateReasons = "";    //列名为：dailyBudgetAmount
            } else {
                JSONArray servingStateReasonsJSONArray = jsonObject.getJSONArray("servingStateReasons");
                JSONArray jsonArray2 = new JSONArray();
                for (Object o : servingStateReasonsJSONArray) {
                    jsonArray2.put(o);
                }
                servingStateReasons = jsonArray2.toString(); //列名为：servingStateReasons
            }


            String servingStatus = jsonObject.getString("servingStatus");       //列名为：servingStatus


            //为空判断
            boolean startTimeIsNull = jsonObject.isNull("startTime");
            String startTime;     //列名为：startTime
            if (startTimeIsNull) {
                startTime = "";
            } else {
                startTime = jsonObject.getString("startTime");
            }

            String status = jsonObject.getString("status");//列名为：status

            //supplySources  拼接
            JSONArray supplySourcesString = jsonObject.getJSONArray("supplySources");
            //for循环拼接
            JSONArray jsonArrayX = new JSONArray();
            for (Object o : supplySourcesString) {
                jsonArrayX.put(o);
            }
            String supplySources = jsonArrayX.toString(); //列名为：str

            //设置rowKey
            Put put = new Put(Bytes.toBytes(cp_id));
            //添加每一个列
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("CERT"), Bytes.toBytes(cert));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("ORG_ID"), Bytes.toBytes(org_id));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("TS"), Bytes.toBytes(ts));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("ADCHANNELTYPE"), Bytes.toBytes(adChannelType));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("ADAMID"), Bytes.toBytes(adamId));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("BUDGETAMOUNT"), Bytes.toBytes(budgetAmount));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("BUDGETORDERS"), Bytes.toBytes(budgetOrders));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("COUNTRIESORREGIONS"), Bytes.toBytes(countriesOrRegions));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("COUNTRYORREGIONSERVINGSTATEREASONS"), Bytes.toBytes(countryOrRegionServingStateReasons));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("DELETED"), Bytes.toBytes(deleted));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("DAILYBUDGETAMOUNT"), Bytes.toBytes(dailyBudgetAmount));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("DISPLAYSTATUS"), Bytes.toBytes(displayStatus));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("ENDTIME"), Bytes.toBytes(endTime));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("CLIENTNAME"), Bytes.toBytes(clientName));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("ORDERNUMBER"), Bytes.toBytes(orderNumber));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("BUYERNAME"), Bytes.toBytes(buyerName));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("BUYEREMAIL"), Bytes.toBytes(buyerEmail));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("BILLINGCONTACTEMAIL"), Bytes.toBytes(billingContactEmail));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("MODIFICATIONTIME"), Bytes.toBytes(modificationTime));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("NAME"), Bytes.toBytes(name));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("PAYMENTMODEL"), Bytes.toBytes(paymentModel));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("SAPINLAWRESPONSE"), Bytes.toBytes(sapinLawResponse));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("SERVINGSTATEREASONS"), Bytes.toBytes(servingStateReasons));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("SERVINGSTATUS"), Bytes.toBytes(servingStatus));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("STARTTIME"), Bytes.toBytes(startTime));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("STATUS"), Bytes.toBytes(status));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("SUPPLYSOURCES"), Bytes.toBytes(supplySources));
            puts.add(put);

        }
        try {
            meta_campaign.put(puts);
        } catch (IOException e) {
            e.printStackTrace();
        }
        puts.clear();
        HbaseUtil.close(hbaseConnection);
    }
}
