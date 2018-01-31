package org.apache.hadoop.hive.druid;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.druid.struct.DruidStruct;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedSerde;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;
import org.stringtemplate.v4.ST;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * <pre>
 *     Druid 数据解析工具
 * </pre>
 *
 * @author saligia
 * @date 18-1-23
 */
@SerDeSpec(schemaProps = {
        DruidSerDe.DRUID_TIMESTAMP_COL
        })
public class DruidSerDe extends AbstractSerDe {



    private List<String> columnNames;         // 记录所有的列名
    private List<String> columnComments;      // 记录所有的列说明
    private List<ObjectInspector> columnOIs;  // 记录所有的列解析
    private List<String> columnTypes;         // 记录所有的列格式
    private int numCols;                      // 记录列数量
    private String timeColmn;                    // 时间范围列

    public static final String DRUID_TIMESTAMP_COL = "_druid_timestamp";

    private ObjectInspector inspector = null;
    private final DruidSerdeRow row = new DruidSerdeRow();

    final class DruidSerdeRow implements Writable {
        Object realRow;
        ObjectInspector inspector;

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            throw new UnsupportedOperationException("can't write the bundle");
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            throw new UnsupportedOperationException("can't read the bundle");
        }

        ObjectInspector getInspector() {
            return inspector;
        }

        Object getRow() {
            return realRow;
        }
    }


    /**
     * <pre>
     *     初始化相关属性信息
     * </pre>
     *
     * @author saligia
     * @param conf 作业配置信息
     * @param table　表属性信息
     * @return　
     */
    @Override
    public void initialize(Configuration conf, Properties table) throws SerDeException {
        columnNames = new ArrayList<String>();
        columnComments = new ArrayList<String>();
        columnOIs = new ArrayList<ObjectInspector>();
        columnTypes = new ArrayList<String>();

        // Read the configuration parameters
        String columnNameProperty = table.getProperty(serdeConstants.LIST_COLUMNS);
        // NOTE: if "columns.types" is missing, all columns will be of String type
        String columnTypeProperty = table.getProperty(serdeConstants.LIST_COLUMN_TYPES);
        timeColmn = table.getProperty(DRUID_TIMESTAMP_COL);


        if(columnNameProperty == null || columnTypeProperty == null){
            throw new SerDeException("columns or columns.types is missing");
        }

        String [] colNames = columnNameProperty.split(",");
        String [] colTypes = columnTypeProperty.split(":");

        if(colNames.length != colTypes.length){
            throw new SerDeException("columns or columns.types is error");
        }

        for(int i = 0 ; i < colNames.length; i ++){
            addColumn(colNames[i], colTypes[i], null);
        }

        numCols = columnNames.size();
        this.inspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, columnOIs,
                columnComments);
    }

    public String getTimeColumn(){
        return timeColmn;
    }

    public void addColumn(String name, String type, String comment) {
        if ("".equals(name))
            return;
        columnNames.add(name);

        if (comment == null)
            comment = "";

        columnComments.add(comment);

        ObjectInspector reflectionObjectInspector = ObjectInspectorFactory.getReflectionObjectInspector(String.class,
                ObjectInspectorOptions.JAVA);

        if (type.equals("string"))
            columnOIs.add(reflectionObjectInspector);
        else if (type.equals("int"))
            columnOIs.add(ObjectInspectorFactory.getReflectionObjectInspector(int.class, ObjectInspectorOptions.JAVA));
        else if (type.equals("long"))
            columnOIs.add(ObjectInspectorFactory.getReflectionObjectInspector(long.class, ObjectInspectorOptions.JAVA));
        else if (type.equals("float"))
            columnOIs
                    .add(ObjectInspectorFactory.getReflectionObjectInspector(float.class, ObjectInspectorOptions.JAVA));
        else if (type.equals("double"))
            columnOIs.add(
                    ObjectInspectorFactory.getReflectionObjectInspector(double.class, ObjectInspectorOptions.JAVA));

        columnTypes.add(type);
    }


    @Override
    public Class<? extends Writable> getSerializedClass() {
        return DruidSerdeRow.class;
    }

    @Override
    public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
        row.realRow = obj;
        row.inspector = objInspector;
        return row;
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return inspector;
    }

    @Override
    public SerDeStats getSerDeStats() {
        return null;
    }

    @Override
    public Object deserialize(Writable writable) throws SerDeException {
        return ((DruidStruct)writable).getFields();
    }
}
