package org.apache.doris.datasource.jdbc.client;

import org.apache.doris.classloader.ChildFirstClassLoader;
import org.apache.doris.cloud.security.SecurityChecker;
import org.apache.doris.jdbc.DriverShim;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Enumeration;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;


public class JdbcTest {

    private static final ConcurrentHashMap<String, Driver> driverMap = new ConcurrentHashMap<>();
    private static final String JDBC_URL = "jdbc:mysql://127.0.0.1:3306/test?useSSL=false";
    private static final Properties CONNECTION_PROPERTIES = createConnectionProperties();

    public static void main(String[] args) throws Exception {
        loadDrivers();

        displayRegisteredDrivers();

        System.out.println("Connection established successfully!");

        for (int i = 0; i < 5; i++) {
            startQueryThread(i);
        }

        while (true) {
            Thread.sleep(1000);
        }
    }

    private static Properties createConnectionProperties() {
        Properties info = new Properties();
        info.put("user", "root");
        info.put("password", "");
        return info;
    }

    private static void loadDrivers() {
        String[] driverPaths = {
                "/Users/zyk/LocalDoris/doris-run/jdbc_drivers/mysql-connector-j-8.3.0.jar",
                "/Users/zyk/LocalDoris/doris-run/jdbc_drivers/mysql-connector-j-8.0.31.jar"
        };
        String driverClassName = "com.mysql.cj.jdbc.Driver";

        for (String path : driverPaths) {
            loadDriver(path, driverClassName);
        }
    }

    private static void loadDriver(String jarPath, String driverClassName) {
        try {
            ChildFirstClassLoader classLoader = new ChildFirstClassLoader(jarPath);
            Class<?> driverClass = classLoader.loadClass(driverClassName);
            Driver driver = (Driver) driverClass.getDeclaredConstructor().newInstance();

            DriverManager.registerDriver(new DriverShim(driver));
            driverMap.put(jarPath, driver);
            System.out.println("Registered driver: " + driverClassName + " from " + jarPath);
        } catch (Exception e) {
            System.err.println("Failed to load driver from " + jarPath);
            e.printStackTrace();
        }
    }

    private static void displayRegisteredDrivers() {
        Enumeration<Driver> drivers = DriverManager.getDrivers();
        while (drivers.hasMoreElements()) {
            Driver driver = drivers.nextElement();
            System.out.println("Registered Driver: " + driver.getClass().getName());
        }
    }

    private static void startQueryThread(int index) {
        new Thread(() -> {
            String jarPath = (index % 2 == 0)
                    ? "/Users/zyk/LocalDoris/doris-run/jdbc_drivers/mysql-connector-j-8.3.0.jar"
                    : "/Users/zyk/LocalDoris/doris-run/jdbc_drivers/mysql-connector-j-8.0.31.jar";

            Driver driverInstance = driverMap.get(jarPath);
            if (driverInstance != null) {
                System.out.println("Attempting to connect using: " + driverInstance);
                try (Connection connection = driverInstance.connect(
                        SecurityChecker.getInstance().getSafeJdbcUrl(JDBC_URL), CONNECTION_PROPERTIES);

                        Statement statement = connection.createStatement();
                        ResultSet resultSet = statement.executeQuery("SELECT * FROM test limit 1")) {

                    if (resultSet.next()) {
                        System.out.println(
                                "Column 1: " + resultSet.getString(1) + " from thread " + Thread.currentThread()
                                        .getName());
                    }
                } catch (Exception e) {
                    System.err.println("Query execution failed in thread " + Thread.currentThread().getName());
                    e.printStackTrace();
                }
            } else {
                System.err.println("Driver not found for path: " + jarPath);
            }
        }).start();
    }
}

