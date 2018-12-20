import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.*;
import java.util.ArrayList;

public class GetFileAndPutJson {
    public static void main(String[] args) throws IOException {
        String serviceName = "es";
        String region = "us-east-1";
        String aesEndpoint;
        String dir;
        if (args.length == 0) {
            aesEndpoint = "https://search-big-data-es-xfksp5mw6zwe6mjsnqm4x6bmea.us-east-1.es.amazonaws.com";
            dir = "/home/headon/ParseHHnew/test_dir/";
        } else if (args.length == 2) {
            dir = args[0];
            aesEndpoint = args[1];
        } else {
            System.out.println("Wrong input");
            return;
        }
        System.out.println("Dir : " + dir);
        System.out.println("ES Endpoint:" + aesEndpoint);


        Path testDir = Paths.get(dir);
        try {
            System.out.println(testDir.toRealPath());
        } catch (IOException e) {
            System.err.println("Wrong path to watched directory (maybe dir doesn't exists)");
            e.printStackTrace();
            return;
        }
        ArrayList<Path> processedFiles = new ArrayList<Path>();

        try {
            Files.walk(testDir).filter(Files::isRegularFile).forEach((file -> {
                RestHighLevelClient client = AWS.esClient(serviceName, region, aesEndpoint);
                try {
                    processFile(file.toString(), client);
                } catch (IOException e) {
                    e.printStackTrace();
                    return;
                }
                processedFiles.add(file);
            }));
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Can't walk " + testDir + " dir");
            return;
        }
        System.out.println("Processed files: ");
        processedFiles.forEach((p) -> {
            System.out.println(p);
        });
    }

    public static void processFile(String fileName, RestHighLevelClient client) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(fileName));

        JSONObject obj = new JSONObject(new JSONTokener(reader));
        JSONArray items = obj.getJSONArray("Items");

        System.out.println("JSONArray size :" + items.length());

        for (Object item : items) {
            int id = ((JSONObject) item).getInt("id");
            addLocationAndWriteToFile((JSONObject) item, id, client);
        }
    }

    public static void addLocationAndWriteToFile(JSONObject readedObject, int id,
                                                 RestHighLevelClient client) throws IOException {
        Object adr = readedObject.get("address");
        if (adr != JSONObject.NULL) {
            JSONObject newAddress = (JSONObject) adr;
            if (newAddress.get("lng") != JSONObject.NULL && newAddress.get("lat") != JSONObject.NULL) {
                double lng = newAddress.getDouble("lng");
                double lat = newAddress.getDouble("lat");
                String result = "{\"lat\":" + lat + ",\"lon\":" + lng + "}";
                JSONObject geo_point = new JSONObject(result);
                readedObject.put("location", geo_point);
            }
        }
        putToEs(readedObject, id, client);
    }

    private static void putToEs(JSONObject readedObject, int id, RestHighLevelClient client) {
        String idStr = Integer.toString(id);
        IndexRequest request = new IndexRequest(
                "my_index",
                "vacancies",
                idStr);
        request.source(readedObject.toString(), XContentType.JSON);
        IndexResponse ir = null;
        try {
            ir = client.index(request);
        } catch (IOException e) {
            System.err.println("Failed to put json to ES");
            System.err.println("Json id:" + id);
            e.printStackTrace();
        }
        System.out.println(ir);
    }
}
