package org.apache.hadoop.hive.druid.util;


import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * <pre>
 *    Segment 元信息
 * </pre>
 *
 * @author saligia
 * @date 18-2-1
 */
public class SegmentDesc {

    public static final String SEGMENT_DESC = "/descriptor.json";
    public static final String SEGMENT_FILE = "/index.zip";

    public static final String DIMENSIONS = "dimensions";
    public static final String METRICS = "metrics";
    public static final String DATASOURCE ="dataSource";
    public static final String INTERVAL = "interval";
    public static final String STORETYPE = "type";
    public static final String STOREPATH = "path";
    public static final String SEGMENTSIZE = "size";

    public static final int NULL_INDEX = -51;

    public static final String DRUID_TIMESTAMP_COL = "_druid_timestamp";

    private String dataSource = null;
    private String timeColumn = null;
    private Set<String> dimensionColumns = new HashSet<String>();
    private Set<String> metrics = new HashSet<String>();
    private DEEPSTORETYPE stopreType = null;
    private String stopPath = null;
    private int size = 0;

    private static enum DEEPSTORETYPE{
        S3("s3"),
        HDFS("hdfs");
        private String storeType = "";

        DEEPSTORETYPE(String storeType){
            this.storeType = storeType;
        }

        public String getStoreType(){
            return storeType;
        }
    }

    public static enum ColumnType{
        DIMENSSION,
        METRIC;
    }

    public SegmentDesc(JsonNode segmentNode){

        if(segmentNode.has(DATASOURCE)){
            this.dataSource = segmentNode.get(DATASOURCE).asText();
        }

        if(segmentNode.has(DIMENSIONS)){
            String[] dimColStrs = segmentNode.get(DIMENSIONS).asText().split(",");
            for(String col : dimColStrs){
                this.dimensionColumns.add(col);
            }
        }

        if(segmentNode.has(METRICS)){
            String [] metricStrs = segmentNode.get(METRICS).asText().split(",");
            for(String col  : metricStrs){
                this.metrics.add(col);
            }
        }

        if(segmentNode.has(SEGMENTSIZE)){
            this.size = segmentNode.get(SEGMENTSIZE).asInt();
        }

        if(segmentNode.has("loadSpec")){
            if(segmentNode.get("loadSpec").has(STORETYPE)){
                if(segmentNode.get("loadSpec").get(STORETYPE).asText().equals(DEEPSTORETYPE.HDFS.getStoreType())){
                    this.stopreType = DEEPSTORETYPE.HDFS;
                }else if(segmentNode.get("loadSpec").get(STORETYPE).asText().equals(DEEPSTORETYPE.S3.getStoreType())){
                    this.stopreType = DEEPSTORETYPE.S3;
                }else{
                    throw new IllegalArgumentException("Segment Storage type is error");
                }
            }

            if(segmentNode.get("loadSpec").has(STOREPATH)){
                this.stopPath = segmentNode.get("loadSpec").get(STOREPATH).asText();
            }
        }
    }

    /**
     * <pre>
     *     根据列标识，返回列类型
     *     如果是未知的列，则返回异常
     * </pre>
     *
     * @author saligia
     * @param column 位置类型的列名称
     * @return ColumnType 列类型
     */
    public ColumnType checkColumn(String column) throws IOException {
        if(this.dimensionColumns.contains(column)){
            return ColumnType.DIMENSSION;
        }

        if(this.metrics.contains(column)){
            return ColumnType.METRIC;
        }

        throw new IOException("Invalid column name");
    }

}
