package org.apache.hadoop.hive.druid.util;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * <pre>
 * </pre>
 *
 * @author saligia
 * @date 18-1-25
 */
public class DruidSplit extends FileSplit{
    public Map<String,String> properties = new HashMap<String, String>();

    public DruidSplit(){
        super(null, 0, 0, (String[]) null);
    }

    public void init(){
    }
    public DruidSplit(Path file, long start, long length, String[] hosts) {
        super(file, start, length, hosts);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        /**
         *  Text.writeString(out, file.toString());
         *  out.writeLong(start);
         *  out.writeLong(length);
         */
        super.write(out);
        Text.writeString(out, toJSON(properties, false));
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        /**
         * file = new Path(Text.readString(in));
         * start = in.readLong();
         * length = in.readLong();
         * hosts = null;
         */
        super.readFields(in);
        String propertyJson = Text.readString(in);
        parseMapFromString(propertyJson);
    }

    public void put(String key, String value){
        properties.put(key, value);
    }
    public String get(String key){
        return properties.get(key);
    }

    public Set<String> getKeys(){
        return properties.keySet();
    }

    private String toJSON(Object obj, boolean prettyPrint) {
        ObjectMapper mapper = new ObjectMapper();

        try {
            if (prettyPrint) {
                ObjectWriter writer = mapper.writerWithDefaultPrettyPrinter();
                return writer.writeValueAsString(obj);
            }
            return mapper.writeValueAsString(obj);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    public void parseMapFromString(String json) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = new JsonFactory();
        JsonParser parser = factory.createJsonParser(json);
        JsonNode node = mapper.readTree(parser);

        Iterator<String> keys = node.fieldNames();

        while(keys.hasNext()){
            String key = keys.next();
            String value = node.get(key).asText();
            properties.put(key, value);
        }
    }
}
