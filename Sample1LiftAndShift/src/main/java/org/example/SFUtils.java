package org.example;

import java.util.*;
import java.io.*;

import com.snowflake.snowpark_java.Session;
import com.snowflake.snowpark_java.types.SnowflakeFile;

public class SFUtils {

    public static boolean is_running_in_sf() {
        return System.getProperty("com.snowflake.import_directory") != null;
    }

    public static InputStream open(String path) throws FileNotFoundException {
            if (is_running_in_sf())
                return SnowflakeFile.newInstance(path, false).getInputStream();
            else
                return new FileInputStream(path);
    }

    public static Session getSession() {
        return createSessionFromSNOWSQLEnvVars();
    }

    public static Session getSession(Boolean usePropertiesFile) {
        if (usePropertiesFile) {
            return createSessionFromPropertiesFile();
        } else {
            return createSessionFromSNOWSQLEnvVars();
        }
    }
    
    private static Session createSessionFromSNOWSQLEnvVars() {
        Map<String, String> configMap = new HashMap<>() {{
            put("URL", getEnv("SNOWSQL_ACCOUNT") + ".snowflakecomputing.com");
            put("USER", getEnv("SNOWSQL_USER"));
            // If you want to use SSO then you can just remove the password setting
            // and add
            // put("AUTHENTICATOR","externalbrowser");
            put("PASSWORD", getEnv("SNOWSQL_PWD"));
            put("DB", getEnv("SNOWSQL_DATABASE"));
            put("SCHEMA", getEnv("SNOWSQL_SCHEMA"));
            put("WAREHOUSE", getEnv("SNOWSQL_WAREHOUSE"));
            put("ROLE", getEnv("SNOWSQL_ROLE"));
        }};

        System.out.println(configMap);

        return Session.builder().configs(configMap).create();
    }

    private static Session createSessionFromPropertiesFile() {
        try {
            String password = System.getenv("SNOWSQL_PWD");

            return Session.builder()
                .configFile("dev.properties")
                .config("password", password)
                .create();
        } catch (NullPointerException e) {
            System.out.println("ERROR: Environment variable, SNOWSQL_PWD, not found. " +
            "Please set this variable");
            e.printStackTrace();
            return null;
        }
    }

    private static String getEnv(String key) throws NullPointerException {
        String val = System.getenv(key);
        if (val == null) {
            throw new NullPointerException(String.format("Environment variable, %s, not found.", key));
        } else {
            return val;
        }
    }
}
