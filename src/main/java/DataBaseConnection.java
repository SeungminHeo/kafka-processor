import java.sql.*;

class DataBaseConnection {
    private String dbUrl;
    private String dbUser;
    private String dbPassword;
    private Connection connectionObject;

    DataBaseConnection(String url, String user, String password) {
        dbUrl = url;
        dbUser = user;
        dbPassword = password;
    }

    DataBaseConnection() {
        /*
         * 임시용, 나중에 설정 Configuration을 바깥으로 뺴야함.
         * 끝나면 이 Overload constructor를 지우고 위에것만 사용!
         */
        dbUrl = "jdbc:mysql://db-4rl2m.cdb.ntruss.com:10024/tmp";
        dbUser = "dtradmin";
        dbPassword = "Whfvm1234";
    }

}