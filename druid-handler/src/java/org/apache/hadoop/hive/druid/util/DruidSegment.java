package org.apache.hadoop.hive.druid.util;
import org.apache.hadoop.io.Writable;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <pre>
 *     存放　Druid 的数据类型
 * </pre>
 *
 * @author saligia
 * @date 18-1-23
 */
public class DruidSegment implements Writable {
    private Object[] fields ;
    private ColumnTypeMap[] colDescs ;
    private Map<String, String> colMap = new HashMap<String,String>();

    public DruidSegment(int colNum, String[] names, String[] types){
        fields = new Object[colNum];
        colDescs = new ColumnTypeMap[colNum];

        for(int i = 0 ; i < colNum; i++){
            colDescs[i] = new ColumnTypeMap(names[i], types[i]);
            colMap.put(names[i], types[i]);
        }
    }

    public static class ColumnTypeMap{
        private String colName ;
        private String colType ;

        public ColumnTypeMap(String name, String colType){
            this.colName = name;
            this.colType = colType;
        }

        public String getColName() {
            return colName;
        }

        public String getColType() {
            return colType;
        }
    }

    public String getFileldName(int index){
        return colDescs[index].getColName();
    }

    public String getFileldTypeById(int index){
        return colDescs[index].getColType();
    }

    public String getFieldTypeByName(String name){
        return colMap.get(name);
    }

    public Object[] getFields() {
        return fields;
    }

    public void fillAllFields(Object[] fields) {
        if(fields == null || this.fields == null){
            throw new IllegalArgumentException("Druid Struct Continer is null");
        }

        if(fields.length <=  this.fields.length){
            throw new IllegalArgumentException("Fields length is so short");
        }

        for(int i =0 ; i < this.fields.length; i++){
            this.fields[i] = fields[i];
        }
    }

    public void fillAllFields(List<Object> fields) {
        if(fields == null || this.fields == null){
            throw new IllegalArgumentException("Druid Struct Continer is null");
        }

        if(fields.size() <=  this.fields.length){
            throw new IllegalArgumentException("Fields length is so short");
        }

        for(int i =0 ; i < this.fields.length; i++){
            this.fields[i] = fields.get(i);
        }
    }

    public void setFields(Object obj, int col){
        if(col >= this.fields.length){
            throw new IllegalArgumentException("Column index out of Range");
        }
        this.fields[col] = obj;
    }

    public int getFieldSize(){
        return fields.length;
    }

    public Object getField(int col){
        if(col >= this.fields.length){
            throw new IllegalArgumentException("Column index out of Range");
        }

        return this.fields[col];
    }

    @Override
    public String toString() {
        ObjectMapper mapper = new ObjectMapper();

        try {
            return mapper.writeValueAsString(fields);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        throw new UnsupportedOperationException("write unsupported");
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        throw new UnsupportedOperationException("write unsupported");
    }
}