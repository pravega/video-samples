package io.pravega.example.tensorflow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public class MySQLConnection {
    private static Logger log = LoggerFactory.getLogger(MySQLConnection.class);
    private Connection _conn;

    public void main(String[] args) throws SQLException {
        Connection con = getRemoteConnection();
        String query = "CREATE TABLE pet (name VARCHAR(20), owner VARCHAR(20),\n" +
                "       species VARCHAR(20), sex CHAR(1), birth DATE, death DATE);";
        assert con != null;
        Statement st = con.createStatement();
        ResultSet rs = st.executeQuery(query);
        ResultSetMetaData rsmd = rs.getMetaData();

        int columnsNumber = rsmd.getColumnCount();
        while (rs.next()) {
            //Print one row
            for(int i = 1 ; i <= columnsNumber; i++){
                System.out.print(rs.getString(i) + " ");
            }
            System.out.println();
        }
    }

    private Connection getRemoteConnection() {
        if (System.getenv("RDS_HOSTNAME") != null) {
            try {
                Class.forName("com.mysql.jdbc.Driver");
                String dbName = System.getenv("RDS_DB_NAME");
                String userName = System.getenv("RDS_USERNAME");
                String password = System.getenv("RDS_PASSWORD");
                String hostname = System.getenv("RDS_HOSTNAME");
                String port = System.getenv("RDS_PORT");
                String jdbcUrl = "jdbc:postgresql://" + hostname + ":" + port + "/" + dbName + "?user=" + userName + "&password=" + password;
                log.trace("Getting remote connection with connection string from environment variables.");
                Connection con = DriverManager.getConnection(jdbcUrl);
                log.info("Remote connection successful.");
                return con;
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
        return null;
    }


}
