package org.topleet.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Configuration file.
 */
public class Configuration {

    private static Properties CONFIGURATION = new Properties();

    static {
        try {
            CONFIGURATION.load(new FileInputStream("config.properties"));
        } catch (IOException e) {
            // This might be ok
            CONFIGURATION = new Properties();
            //System.out.println("There is no 'config.properties' file in the projects root folder. System environment will be searched for configuration.");
        }
    }

    public static String get(String key) {
        String result = CONFIGURATION.getProperty(key);
        if (result != null)
            return result;

        if(System.getenv().containsKey(key))
            return System.getenv(key);

        return key;
        //throw new RuntimeException("Missing get key: " + key);
    }

}
