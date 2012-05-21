package dk.statsbiblioteket.mediaplatform.bitrepository.urlclient;

import java.io.File;

import org.bitrepository.protocol.utils.LogbackConfigLoader;
import org.json.JSONException;
import org.json.JSONObject;

public class TheClient {
    
    private static final int CONFIG_DIR_ARG_INDEX = 0;
    private static final int FILE_LOCATION_ARG_INDEX = 1;
    private static final int FILEID_ARG_INDEX = 2;
    private static final int CHECKSUM_ARG_INDEX = 3;
    private static final int FILESIZE_ARG_INDEX = 4;
    
    private TheClient() {}
    
    public static void main(String[] args) {
        verifyInputParams(args);
        setupLogging(args[CONFIG_DIR_ARG_INDEX]);
        try {
            FilePutter putter = new FilePutter(args[CONFIG_DIR_ARG_INDEX], args[FILEID_ARG_INDEX], 
                    args[FILE_LOCATION_ARG_INDEX], args[CHECKSUM_ARG_INDEX], 
                    Long.parseLong(args[FILESIZE_ARG_INDEX]));  
            putter.putFile();
            
            JSONObject obj = new JSONObject();
            try {
                obj.put("UrlToFile", putter.getUrl());
            } catch (JSONException e) {
                System.exit(ClientExitCodes.JSON_ERROR);
            }
            System.out.println(obj.toString());
            System.exit(ClientExitCodes.SUCCESS);
        } catch (ClientFailureException e) {
            System.out.println(e.getMessage());
            System.exit(e.getExitCode());
        }
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
            System.exit(ClientExitCodes.INPUT_PARAM_COUNT_ERROR);
        }
        
        File configDir = new File(args[CONFIG_DIR_ARG_INDEX]);
        if(!configDir.isDirectory()) {
            System.out.println("Config dir parameter (parm " + CONFIG_DIR_ARG_INDEX + ") is no directory!");
            System.exit(ClientExitCodes.CONFIG_DIR_ERROR);
        }
        if(!configDir.canRead()) {
            System.out.println("Config dir '" + args[CONFIG_DIR_ARG_INDEX] + "' cannot be read!");
            System.exit(ClientExitCodes.CONFIG_DIR_ERROR);
        }
        
        try {
                Long.parseLong(args[FILESIZE_ARG_INDEX]);
        } catch (Exception e) {
            System.out.println("Failed to parse filesize argument as long.");
            System.exit(ClientExitCodes.FILE_SIZE_ERROR);
        }
        
        String checksum = args[CHECKSUM_ARG_INDEX];
        if((checksum.length() % 2) != 0) {
            System.out.println("Checksum argument does not contain an even number of characters.");
            System.exit(ClientExitCodes.CHECKSUM_ERROR);
        }
        if(!checksum.matches("^\\p{XDigit}*$")) {
            System.out.println("Checksum argument contains non hexadecimal value!");
            System.exit(ClientExitCodes.CHECKSUM_ERROR);
        } 
    }
    
    private static void setupLogging(String configDir) {
        try {
            new LogbackConfigLoader(configDir + "/logback.xml");
        } catch (Exception e) {
            // Yes, indeed do nothing, don't want to pollute stdout. 
            // Or perhaps we should return an error code indicating log setup failure?
        } 
    }
}
