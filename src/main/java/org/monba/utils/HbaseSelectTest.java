package org.monba.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseSelectTest {

    //TODO 单条数据查询(GET) 单行多列族
    public static void getDate(String tableName, String rowKey, String cf) throws IOException {

        //1.获取配置信息并设置连接参数
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "172.31.25.167");

        //2.获取连接
        Connection connection = ConnectionFactory.createConnection(configuration);

        //3.获取表的连接
        Table table = connection.getTable(TableName.valueOf(tableName));

        //4.创建Get对象
        Get get = new Get(Bytes.toBytes(rowKey));
        // 指定列族查询
        get.addFamily(Bytes.toBytes(cf));
        // 指定列族:列查询
        // get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));

        //5.查询数据
        Result result = table.get(get);

        //6.解析result
        for (Cell cell : result.rawCells()) {
            System.out.println("CF:" + Bytes.toString(cell.getFamilyArray()) +
                    ",CN:" + Bytes.toString(cell.getQualifierArray()) +
                    ",Value:" + Bytes.toString(cell.getValueArray()));
        }
        //7.关闭连接
        table.close();
        connection.close();
    }

    public static void main(String[] args) throws IOException {
        HbaseSelectTest.getDate("REPORT_KEYWORD","495486557_2020-11-20","info");
    }

}
