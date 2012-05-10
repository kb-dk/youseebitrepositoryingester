package dk.statsbiblioteket.mediaplatform.bitrepository.urlclient;

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
        if(args.length != 5) {
            System.out.println("Unexpected number of arguments, got " + args.length + " but expected 5");
            System.out.println("Expecting: ConfigDirPath FileUrl FileID FileChecksum FileSize");
            System.exit(1);
        } else {
            JSONObject obj = new JSONObject();
            try {
                obj.put("UrlToFile", "http://bitrepository.org/" + args[FILEID_ARG_INDEX]);
            } catch (JSONException e) {
                System.exit(2);
            }
            System.out.println(obj.toString());
        }
        System.exit(0);
    }
}
