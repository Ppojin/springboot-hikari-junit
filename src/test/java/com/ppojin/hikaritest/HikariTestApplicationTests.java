package com.ppojin.hikaritest;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.boot.test.context.SpringBootTest;

import java.sql.*;
import java.util.concurrent.*;


@SpringBootTest
@Slf4j
class HikariTestApplicationTests {

    private static HikariDataSource dataSource;
    long beforeTime;

    @BeforeAll
    static void beforeAll() {
    }

    @AfterAll
    static void afterAll() {
    }

    @BeforeEach
    void setUp() {
        HikariConfig config = new HikariConfig();
//        config.setJdbcUrl("jdbc:postgresql://localhost:5432/postgres");
//        config.setUsername("pguser");
//        config.setPassword("pwd");
//        config.setDriverClassName("oracle.jdbc.OracleDriver");

        config.setJdbcUrl("jdbc:oracle:thin:@//localhost:4321/ORCLPDB1?oracle.jdbc.ReadTimeout=5000ms");
        config.setUsername("TESTUSER");
        config.setPassword("pwd");

//
//        config.addDataSourceProperty("oracle.net.keepAlive", true);
//        config.addDataSourceProperty("oracle.net.TCP_KEEPIDLE", 1);
//        config.addDataSourceProperty("oracle.net.TCP_KEEPINTERVAL", 1);
//        config.addDataSourceProperty("oracle.net.TCP_KEEPCOUNT", 1);
//        config.addDataSourceProperty("oracle.net.CONNECT_TIMEOUT", "500ms");
//
        config.addDataSourceProperty("oracle.jdbc.ReadTimeout", "5000ms");
//        config.addDataSourceProperty("socketTimeout", 3);

        config.setMinimumIdle(1);
        config.setMaximumPoolSize(1);

//        config.setConnectionTimeout(250);
        config.setPoolName("테스트풀");

        config.setKeepaliveTime(3000);

        dataSource = new HikariDataSource(config);

        log.info(dataSource.getPoolName());
        System.out.println("::start test::");
        beforeTime = System.currentTimeMillis();
        timeDiff("test start");
    }

    @AfterEach
    void tearDown() {
        System.out.println("::end test::");
        dataSource.close();
    }

    void timeDiff(String message){
        log.info(">>> {}: {}ms", message, System.currentTimeMillis() - beforeTime);
        beforeTime = System.currentTimeMillis();
    }

    void healthCheck(){
        log.info("===============================================");
        timeDiff("healthCheck start");
        try (
                Connection conn = dataSource.getConnection();
                Statement stmt = conn.createStatement();
        ){
            ResultSet rs = stmt.executeQuery("select 'hi' from dual");
//            ResultSet rs = stmt.executeQuery("select 'hi'");
            rs.next();
            log.info("nextRs = {}", rs.getString(1));
            log.info("connIsClosed {}", conn.isClosed());
        } catch (SQLException e) {
//            log.error(e.getMessage());
        }
        timeDiff("healthCheck done");
        log.info("===============================================");
    }

    void executeSleepQuery(Statement stmt, int queryTimeSec) throws SQLException {
        timeDiff("executeSleepQuery start");
        try {
//            String sql = String.format("select %d, pg_sleep(%d)", queryTimeSec, queryTimeSec);
//            String sql = "select 1 from dual";
            String sql = "SELECT sys.hj_sleep("+queryTimeSec+") FROM dual";
//            String sql = "begin dbms_lock.sleep(1); end; select 1 from dual;";
//            String sql = String.format("select %d from dual", queryTimeSec);
            log.debug("sql: ({})", sql);
            ResultSet rs = stmt.executeQuery(sql);
            if(rs.next()){
                log.info("delayRs = {}", rs.getLong(1));
            }
        } catch (SQLException e) {
            throw e;
        } finally {
            timeDiff("executeSleepQuery done");
        }
    }

    @Test
    void onlyOneConnectionTest() {
        try (Connection conn = dataSource.getConnection();) {
            System.out.println(">>>>1");
            Connection conn2 = dataSource.getConnection();
            System.out.println(">>>>2");
            conn2.close();
        } catch (SQLException e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }

        healthCheck();
    }

//    @CsvSource(value = {"2,1", "2,2", "1,2", "14,15"}, delimiter = ',')
    @CsvSource(value = {"21,20"}, delimiter = ',')
    @ParameterizedTest
    void setNetworkTimeoutTest(int timeOutSec, int queryTimeSec) {
        try (
                Connection conn = dataSource.getConnection();
                Statement stmt = conn.createStatement();
        ){
            conn.setNetworkTimeout(
                    Executors.newSingleThreadExecutor(),
                    timeOutSec * 1000
            );
            stmt.setQueryTimeout(timeOutSec);
            executeSleepQuery(stmt, queryTimeSec);
        } catch (SQLException e) {
            log.error("SQLException: ", e);
        }

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        healthCheck();
    }

    @CsvSource(value = {"1,30", "2,2", "2,1", "16,17", "14,15"}, delimiter = ',')
    @ParameterizedTest
    void setQueryTimeoutTest_2(int timeOutSec, int queryTimeSec) {
        try (Connection conn = dataSource.getConnection()){
            try (Statement stmt = conn.createStatement();) {
                log.debug("stmt timeout: {}", timeOutSec);
                stmt.setQueryTimeout(timeOutSec);
                executeSleepQuery(stmt, queryTimeSec);
            } catch (SQLTimeoutException e) {
                dataSource.evictConnection(conn);
                log.error(e.getMessage());
            }
        } catch (SQLException e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }

        healthCheck();
    }

//    @CsvSource(value = {"1,30", "2,2", "2,1", "16,17", "14,15"}, delimiter = ',')
    @CsvSource(value = {"3,30"}, delimiter = ',')
    @ParameterizedTest
    void setQueryTimeoutTest(int timeOutSec, int queryTimeSec) {
        try (
                Connection conn = dataSource.getConnection();
                Statement stmt = conn.createStatement();
        ){
            System.out.println(dataSource.getDataSourceClassName());
            stmt.setQueryTimeout(timeOutSec);
            executeSleepQuery(stmt, queryTimeSec);
        } catch (SQLException e) {
            log.error("errorMessage: ", e);
        }

        healthCheck();
    }

    //    @CsvSource(value = {"1,30", "2,2", "2,1", "16,17", "14,15"}, delimiter = ',')
    @CsvSource(value = {"3,30"}, delimiter = ',')
    @ParameterizedTest
    void connectionEvictedTest(int timeOutSec, int queryTimeSec) {
        try(Connection conn = dataSource.getConnection();){
            try (Statement stmt = conn.createStatement();){
                System.out.println(dataSource.getDataSourceClassName());
                stmt.setQueryTimeout(timeOutSec);
                executeSleepQuery(stmt, queryTimeSec);
            } catch (SQLException e) {
                dataSource.evictConnection(conn);
            }
        } catch (SQLException e) {
            log.error("errorMessage: ", e);
        }
//        dataSource.close();

        healthCheck();
    }

    @Test
    void futureTest() throws ExecutionException, InterruptedException {
        int sleepTime = 3;

        try (
                Connection conn = dataSource.getConnection();
                Statement stmt = conn.createStatement();
        ){
//            dataSource.evictConnection();
            ExecutorService executor = Executors.newSingleThreadExecutor();
            Future<ResultSet> future = executor.submit(() -> {
                return stmt.executeQuery("SELECT count(1) as count FROM voduser.pt_vo_buy_detail");
            });

            try {
                ResultSet timeoutRs = future.get(1, TimeUnit.SECONDS);
                timeoutRs.next();
                log.info("timeoutRs = {}", timeoutRs.getLong(1));
            } catch (TimeoutException e) {
                future.cancel(true);
//                stmt.getConnection().close();
                dataSource.evictConnection(conn);
            } finally {
                log.info("connIsClosed {}", conn.isClosed());
                executor.shutdownNow();
            }
        } catch (SQLException e) {
            log.error(e.getMessage());
        }

        healthCheck();
    }
}
