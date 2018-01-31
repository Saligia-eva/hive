package org.apache.hadoop.hive.druid.struct;
import org.apache.hadoop.io.Writable;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * <pre>
 *     存放　Druid 的数据类型
 * </pre>
 *
 * @author saligia
 * @date 18-1-23
 */
public class  DruidStruct implements Writable {
    private Object[] fields ;

    public DruidStruct(int colNum){
        fields = new Object[colNum];
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
