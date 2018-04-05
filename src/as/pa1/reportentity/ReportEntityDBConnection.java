package as.pa1.reportentity;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ReportEntityDBConnection {
    private static final String DB_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_CONNECTION = "jdbc:mysql://localhost:3306/reportentity";
    private static final String DB_USER = "g03";
    private static final String DB_PASSWORD = "t2";
    
    private Connection reConnection;
    
    public ReportEntityDBConnection() {
        
    }
    
    public void init() {
        try {
            Class.forName(DB_DRIVER);
            reConnection = DriverManager.getConnection(
                    DB_CONNECTION, DB_USER, DB_PASSWORD);
        } catch (ClassNotFoundException cnfEx) {
            System.out.println("Class not found: "+ cnfEx.getMessage());
        } catch (SQLException sqlEx) {
            System.out.println("SQLException: "+ sqlEx.getMessage());
            System.out.println("SQLState: "+ sqlEx.getSQLState());
            System.out.println("VendorError: "+ sqlEx.getErrorCode());
        } 
    }
    
    public Connection getConnection() {
        return reConnection;
    }
    
    public void close(java.sql.PreparedStatement stmt) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException sqlEx) {
                System.out.println("SQLException: "+ sqlEx.getMessage());
                System.out.println("SQLState: "+ sqlEx.getSQLState());
                System.out.println("VendorError: "+ sqlEx.getErrorCode());
            }
        }
    }
    
    public void destroy() {
        if (reConnection != null) {
            try {
                reConnection.close();
            } catch (SQLException sqlEx) {
                System.out.println("SQLException: "+ sqlEx.getMessage());
                System.out.println("SQLState: "+ sqlEx.getSQLState());
                System.out.println("VendorError: "+ sqlEx.getErrorCode());
            }
        }
    }
}
