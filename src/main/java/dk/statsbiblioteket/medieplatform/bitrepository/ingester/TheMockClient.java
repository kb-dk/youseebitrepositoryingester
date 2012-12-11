package dk.statsbiblioteket.medieplatform.bitrepository.ingester;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.json.JSONException;
import org.json.JSONObject;

import dk.statsbiblioteket.medieplatform.bitrepository.ingester.ClientExitCodes.ExitCodes;

public class TheMockClient {
    
    private static final int CONFIG_DIR_ARG_INDEX = 0;
    private static final int FILE_LOCATION_ARG_INDEX = 1;
    private static final int FILEID_ARG_INDEX = 2;
    private static final int CHECKSUM_ARG_INDEX = 3;
    private static final int FILESIZE_ARG_INDEX = 4;
    public static final String INGESTER_PROPERTIES_FILE = "ingester.properties";
    
    private TheMockClient() {}
    
    public static void main(String[] args) {
        verifyInputParams(args);
        Properties properties = loadProperties(args[CONFIG_DIR_ARG_INDEX]);
        String baseUrl = properties.getProperty("dk.statsbiblioteket.medieplatform.bitrepository.baseurl");
        JSONObject obj = new JSONObject();
        try {
            obj.put("UrlToFile", baseUrl + args[FILEID_ARG_INDEX]);
        } catch (JSONException e) {
            System.exit(ExitCodes.JSON_ERROR.getCode());
        }
        System.out.println(obj.toString());
        System.exit(ExitCodes.SUCCESS.getCode());
    }
    
    
    /**
     * Method to verify the input parameters
     * Verifies the validity of:
     * - Number of arguments 
     * - The existence and readability of the configuration directory
     * - The ability to parse the file size parameter as a long
     * - The length and content of the checksum parameter  
     * If validation fails error is printed to console and program is exited. 
     */
    private static void verifyInputParams(String[] args) {
        if(args.length != 5) {
            System.out.println("Unexpected number of arguments, got " + args.length + " but expected 5");
            System.out.println("Expecting: ConfigDirPath FileUrl FileID FileChecksum FileSize");
            System.exit(ExitCodes.INPUT_PARAM_COUNT_ERROR.getCode());
        }
        
        File configDir = new File(args[CONFIG_DIR_ARG_INDEX]);
        if(!configDir.isDirectory()) {
            System.out.println("Config dir parameter (parm " + CONFIG_DIR_ARG_INDEX + ") is no directory!");
            System.exit(ExitCodes.CONFIG_DIR_ERROR.getCode());
        }
        if(!configDir.canRead()) {
            System.out.println("Config dir '" + args[CONFIG_DIR_ARG_INDEX] + "' cannot be read!");
            System.exit(ExitCodes.CONFIG_DIR_ERROR.getCode());
        }
        
        try {
                Long.parseLong(args[FILESIZE_ARG_INDEX]);
        } catch (Exception e) {
            System.out.println("Failed to parse filesize argument as long.");
            System.exit(ExitCodes.FILE_SIZE_ERROR.getCode());
        }
        
        String checksum = args[CHECKSUM_ARG_INDEX];
        if((checksum.length() % 2) != 0) {
            System.out.println("Checksum argument does not contain an even number of characters.");
            System.exit(ExitCodes.CHECKSUM_ERROR.getCode());
        }
        if(!checksum.matches("^\\p{XDigit}*$")) {
            System.out.println("Checksum argument contains non hexadecimal value!");
            System.exit(ExitCodes.CHECKSUM_ERROR.getCode());
        } 
    }
    
    /**
     * Load properties from configuration file 
     */
    private static Properties loadProperties(String configDir) {
        Properties properties = new Properties();
        try {
            String propertiesFile = configDir + "/" + INGESTER_PROPERTIES_FILE;
            BufferedReader reader = new BufferedReader(new FileReader(propertiesFile));
            properties.load(reader);
            return properties;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
