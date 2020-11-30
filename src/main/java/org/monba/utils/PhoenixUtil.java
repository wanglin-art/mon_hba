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
        //select "avgCPA_Amount" from report_campaign limit 120
        PreparedStatement preparedStatement = connection.prepareStatement("select \"cpaGoal_Amount\" from meta_adgroup limit 120");

        ResultSet resultSet = preparedStatement.executeQuery();

        while (resultSet.next()) {
            System.out.println(resultSet.getDouble(1) );
        }

        //关闭
        connection.close();

    }

}
