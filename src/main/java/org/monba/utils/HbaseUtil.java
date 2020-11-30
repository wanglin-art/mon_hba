package org.monba.utils;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


public class HbaseUtil {
    protected static Configuration conf = null;
    protected static Connection connection = null;
    protected static Admin admin = null;

    /**
     * 取得连接
     */
    public static Connection getHbaseConnection() {
        try {
            conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "172.31.25.167");//zookeeper地址
            conf.set("hbase.zookeeper.property.clientPort","2181");
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return connection;
    }

    /**
     * 关闭连接
     */
    public static void close(Connection connection) {
        try {
            if (connection != null) {
                connection.close();
            }

            if (admin != null) {
                admin.close();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }




}
