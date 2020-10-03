package utils.database;

import java.sql.*;

public class DataBaseConnection {
    private String dbUrl;
    private String dbUser;
    private String dbPassword;
    private Connection connectionObject;

    public DataBaseConnection(String url, String user, String password) {
        dbUrl = url;
        dbUser = user;
        dbPassword = password;
    }

    public DataBaseConnection() {
        /*
         * 임시용, 나중에 설정 Configuration을 바깥으로 뺴야함.
         * 끝나면 이 Overload constructor를 지우고 위에것만 사용!
         */
        dbUrl = "jdbc:mysql://db-4rl2m.cdb.ntruss.com:10024/tmp";
        dbUser = "dtradmin";
        dbPassword = "Whfvm1234";
    }

    public ResultSet loadLogData(String query) throws SQLException {
        try {
            Connection conn = DriverManager.getConnection(this.dbUrl, this.dbUser, this.dbPassword);
            Statement statement = conn.createStatement();
            ResultSet logData = statement.executeQuery(query);
            return logData;
        } catch (SQLTimeoutException e) {
            throw new SQLTimeoutException("Lost Connection for Database. Do Reconnect.");
        } catch (SQLSyntaxErrorException e) {
            throw new SQLSyntaxErrorException("Wrong SQL syntax");
        }
    }

    public ResultSet loadLogData() throws SQLException {
        try {
            Connection conn = DriverManager.getConnection(this.dbUrl, this.dbUser, this.dbPassword);
            Statement statement = conn.createStatement();
            ResultSet logData = statement.executeQuery(
                    "SELECT * FROM logdata WHERE time > '2020-09-21' - INTERVAL 1 day");
            return logData;
        } catch (SQLTimeoutException e) {
            throw new SQLTimeoutException("Lost Connection for Database. Do Reconnect.");
        }
    }
}