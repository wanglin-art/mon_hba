package org.monba.utils;

import org.apache.phoenix.queryserver.client.ThinClientUtil;

import java.io.IOException;
import java.sql.*;


public class PhoenixUtil {
    protected static Connection connection =null;

    public static Connection getConnection() throws SQLException {
        String connectionUrl = ThinClientUtil.getConnectionUrl("172.31.25.167", 8765);
        Connection connection = DriverManager.getConnection(connectionUrl);


        return  connection;
    }
    /**
     * 关闭连接
     */
    public static void close(Connection connection) {
        try {
            if (connection != null) {
                connection.close();
            }


        } catch (SQLException e) {
            e.printStackTrace();
        }
    }



    public static void main(String[] args) throws SQLException {

        String connectionUrl = ThinClientUtil.getConnectionUrl("172.31.25.167", 8765);
        System.out.println(connectionUrl);
        Connection connection = DriverManager.getConnection(connectionUrl);
        //"select \"cpaGoal_Amount\" from meta_adgroup limit 120"
        //"select kw_id from meta_keyword where ad_id = "+ad_id+" and text = " +"\'"+kw+"\'"
        PreparedStatement preparedStatement = connection.prepareStatement("select ADGROUPID,AD_ID,BIDAMOUNT_AMOUNT from report_keyword where id = " +"\'"+"495486557_2020-11-20"+"\'");

        ResultSet resultSet = preparedStatement.executeQuery();

        while (resultSet.next()) {
            System.out.println(resultSet.getString(1) );
            System.out.println(resultSet.getString(2) );
            System.out.println(resultSet.getString(3) );
        }

        //关闭
        connection.close();

    }

}
