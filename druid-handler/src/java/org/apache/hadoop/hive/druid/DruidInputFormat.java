package org.apache.hadoop.hive.druid;

import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.DruidProcessingConfig;
import io.druid.segment.IndexIO;
import io.druid.segment.QueryableIndex;
import io.druid.segment.column.Column;
import io.druid.segment.column.SimpleDictionaryEncodedColumn;
import io.druid.segment.column.ValueType;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.druid.struct.DruidSplit;
import org.apache.hadoop.hive.druid.struct.DruidStruct;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler.DecomposedPredicate;
import org.apache.hadoop.hive.ql.plan.*;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.*;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * <pre>
 *     Druid 数据拉取工具
 *     将数据从　s3拷贝到本地并进行映射, 使用　druid Historical server 的接口，采用　mmp 的方式将数据拉取进来。
 * </pre>
 *
 * @author saligia
 * @date 18-1-23
 */
public class DruidInputFormat implements InputFormat<NullWritable, DruidStruct> {

    /**
     * <pre>
     *     获取　split 的方式：
     *      1. 携带segment 位置信息
     *      2. 携带相关过滤信息
     * </pre>
     *
     * @author saligia
     * @param job 作业相关配置
     * @return
     */
    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {

        // 得到过滤器
        DecomposedPredicate decomposedPredicate = getDecomposePredicate(job);
        // 获取输入路径
        Path[] paths = new Path[0];
        try {
            paths = getInputPaths(job, decomposedPredicate);
        } catch (HiveException e) {
            throw new IOException(e);
        }

        if(paths == null){
            throw new IOException("Input path is null");
        }
        DruidSplit[] intpuSplit = new DruidSplit[paths.length];

        for(int i = 0 ; i < intpuSplit.length; i++){
            intpuSplit[i] = new DruidSplit(paths[i], 0, 0, null);
            intpuSplit[i].setDecomposedPredicate(decomposedPredicate.pushedPredicateObject);
        }

        return intpuSplit;
    }

    public static DecomposedPredicate getDecomposePredicate(JobConf job){
        DecomposedPredicate decomposedPredicate = new DecomposedPredicate();

        // 得到时间过滤
        String filterExprSerialized = job.get(TableScanDesc.FILTER_EXPR_CONF_STR);
        if(filterExprSerialized != null){
            ExprNodeGenericFuncDesc filterExpr =
                    Utilities.deserializeExpression(filterExprSerialized);
            decomposedPredicate.pushedPredicate = filterExpr;
        }

        // 解析后期聚合方式
        String filterObjectSerialized = job.get(TableScanDesc.FILTER_OBJECT_CONF_STR);
        if(filterObjectSerialized != null){
            ExprNodeGenericFuncDesc predicate = Utilities.deserializeExpression(filterObjectSerialized);
            decomposedPredicate.pushedPredicateObject = predicate;
        }

        return decomposedPredicate;
    }

    /**
     * Get the list of input {@link Path}s for the map-reduce job.
     *
     * @param conf The configuration of the job
     * @return the list of input {@link Path}s for the map-reduce job.
     */
    public static Path[] getInputPaths(JobConf conf, DecomposedPredicate decomposedPredicate) throws IOException, HiveException {

        //.../${TableName}/${StartTime}_${EndTime}/${TimeVersion}/${File}
        // 获取输入路径
        String tablePath = conf.get("mapred.input.dir", "");
        FileSystem fs = FileSystem.get(conf);

        // 获取　TableName
        FileStatus fileStatus = fs.getFileStatus(new Path(tablePath));

        if(!fileStatus.isDirectory()){
            throw new IOException("File path Format error");
        }

        // 获取　TableName 下的时间分区
        List<Path> timeFilePath = new LinkedList<Path>();
        FileStatus[] timeFileStatuses = fs.listStatus(fileStatus.getPath());

        for(FileStatus timeFileStatus : timeFileStatuses) {
            if (!timeFileStatus.isDirectory()) {
                continue;
            }

            if (decomposedPredicate.pushedPredicate != null) {
                ExprNodeGenericFuncDesc funcDesc = decomposedPredicate.pushedPredicate;

                String []timePath = timeFileStatus.getPath().getName().split("_");

                DateTime dateTimeStart;  //= DateTime.parse(timePath[0].split("T")[0]);
                DateTime dateTimeStop; // = DateTime.parse(timePath[1].split("T")[0]);

                if(!timeFileStatus.getPath().getName().contains(":")){
                    dateTimeStart = DateTime.parse(timePath[0], DateTimeFormat.forPattern("yyyyMMdd'T'HHmmss.SSSZ"));
                    dateTimeStop = DateTime.parse(timePath[1], DateTimeFormat.forPattern("yyyyMMdd'T'HHmmss.SSSZ"));
                }else{
                    dateTimeStart = DateTime.parse(timePath[0], DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ"));
                    dateTimeStop = DateTime.parse(timePath[1], DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ"));
                }

                if(!(boolean)((ExprNodeConstantDesc)getTimeFilter(dateTimeStart, dateTimeStop, funcDesc)).getValue()){
                    continue;
                }
            }
            timeFilePath.add(timeFileStatus.getPath());  // 插入目录计算
        }

        // 获取最大的时间版本
        List<Path> segmentPaths = new LinkedList<Path>();

        for(Path segmentTimestampPath : timeFilePath){

            FileStatus[] segmentStatuses = fs.listStatus(segmentTimestampPath);
            Map<Long, Path> timeHash = new HashMap<Long, Path>(); //　timestamp -> path
            long maxtimestamp = 0;
            for(FileStatus segmentStatus : segmentStatuses){
                long timeStamp = DateTime.parse(segmentStatus.getPath().getName().replace("_",":")).getMillis();

                if(maxtimestamp < timeStamp){
                    maxtimestamp = timeStamp;
                }
                timeHash.put(timeStamp, segmentStatus.getPath());
            }
            segmentPaths.add(timeHash.get(maxtimestamp));
        }
        // 收集segment
        List<Path> bucketSegment = new LinkedList<Path>();

        for(Path segmentPath : segmentPaths){
            FileStatus[] bucketPaths = fs.listStatus(segmentPath);

            for(FileStatus bucketPath : bucketPaths){
                bucketSegment.add(bucketPath.getPath());
            }
        }

        // 转数组
        Path[] bucketSegmentPaths = new Path[bucketSegment.size()];

        for(int i =0 ; i < bucketSegmentPaths.length; i++){
            bucketSegmentPaths[i] = bucketSegment.get(i);
        }
        return bucketSegmentPaths;
    }

    public static BooleanWritable compareTo(int obj1, int obj2, String op) throws IOException {
        if("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual".equals(op)){
            return new BooleanWritable(obj1 == obj2);
        }else if("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan".equals(op)){
            return new BooleanWritable (obj1 >= obj2);
        }else if("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan".equals(op)){
            return new BooleanWritable (obj1 <= obj2);
        }else if("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan".equals(op)){
            return new BooleanWritable (obj1 < obj2);
        }else if("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan".equals(op)){
            return new BooleanWritable(obj1 > obj2);
        }else if("org.apache.hadoop.hive.ql.udf.GenericUDFOPNotEqual".equals(obj2)){
            return new BooleanWritable(obj1 != obj2);
        }else{
            throw new IOException("Error Operator");
        }
    }

    public static BooleanWritable compareTo(boolean obj1, boolean obj2, String op) throws IOException {
        if("org.apache.hadoop.hive.ql.udf.GenericUDFOPAnd".equals(op)){
            return new BooleanWritable(obj1 || obj2);
        }else if("org.apache.hadoop.hive.ql.udf.GenericUDFOPAnd".equals(op)){
            return new BooleanWritable(obj1 && obj2);
        }else{
            throw new IOException("Error Operator");
        }
    }
    public static ExprNodeDesc getTimeFilter(DateTime startTime, DateTime stopTime, ExprNodeDesc funcDesc) throws HiveException, IOException {

        // 如果是关联类，将获取两个子集
        if (((ExprNodeGenericFuncDesc)funcDesc).getGenericUDF() instanceof GenericUDFOPAnd
                || ((ExprNodeGenericFuncDesc)funcDesc).getGenericUDF() instanceof GenericUDFOPOr){
            // 操作子类任务
            funcDesc.getChildren().set(0, getTimeFilter(startTime,stopTime,funcDesc.getChildren().get(0)));
            funcDesc.getChildren().set(1, getTimeFilter(startTime,stopTime,funcDesc.getChildren().get(1)));

        }
        // 计算外层逻辑
        if(funcDesc.getChildren().get(0) instanceof ExprNodeColumnDesc){
            // 分区带入计算
            Integer timeStartInt = Integer.parseInt(startTime.toString("yyyyMMdd"));
            Integer timeStopInt = Integer.parseInt(stopTime.toString("yyyyMMdd"));

            Integer timeGetInt = (int)((ExprNodeConstantDesc)funcDesc.getChildren().get(1)).getValue();

            BooleanWritable bool1 = compareTo(timeStartInt, timeGetInt, ((ExprNodeGenericFuncDesc) funcDesc).getGenericUDF().getUdfName());

            BooleanWritable bool2 = compareTo(timeStopInt, timeGetInt, ((ExprNodeGenericFuncDesc) funcDesc).getGenericUDF().getUdfName());

            BooleanWritable bool3 = new BooleanWritable(timeStartInt <= timeGetInt && timeGetInt < timeStopInt);

            return new ExprNodeConstantDesc(TypeInfoFactory.booleanTypeInfo,
                    bool1.get() || bool2.get() || bool3.get());
        }

        BooleanWritable boolres = compareTo((Boolean)((ExprNodeConstantDesc)funcDesc.getChildren().get(0)).getValue(),
                (Boolean)((ExprNodeConstantDesc)funcDesc.getChildren().get(1)).getValue(),((ExprNodeGenericFuncDesc) funcDesc).getGenericUDF().getUdfName());

        return new ExprNodeConstantDesc(TypeInfoFactory.booleanTypeInfo, boolres.get());
    }

    private static class DruidRecordReader implements RecordReader<NullWritable, DruidStruct> {
        private QueryableIndex queryableIndex = null;                                     // 获取 segment 的映射接口
        private DruidStruct druidStruct = null;                                           // 数据输出接口
        private String timeColName ;                                                      // 时间列标记
        private int[] colIds;                                                             //
        private String[] colNames;
        private String[] colTypes;
        private File segmentFile;                                                          // segment文件标记
        private int rowNum;                                                                // 记录总的行数量
        private int currentNum;                                                            // 当前读取的行数

        public DruidRecordReader(JobConf conf, File file) throws IOException {
            ObjectMapper objectMapper = new DefaultObjectMapper((JsonFactory) null);
            IndexIO druidIndexIO = new IndexIO( objectMapper,
                    new DruidProcessingConfig() {
                        @Override
                        public String getFormatString() {
                            return null;
                        }
                    });
            this.queryableIndex = druidIndexIO.loadIndex(file);

            this.timeColName = conf.get(DruidSerDe.DRUID_TIMESTAMP_COL);
            this.segmentFile  = file;
            String[] colStr = conf.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR).split(",");
            colIds = new int[colStr.length];
            for(int i = 0 ; i < colIds.length; i++){
                colIds[i] = Integer.parseInt(colStr[i]);
            }

            this.colNames = conf.get("columns").split(",");
            this.colTypes = conf.get("columns.types").split(",");
            this.druidStruct = new DruidStruct(colNames.length);
            this.rowNum = queryableIndex.getNumRows();
            this.currentNum = 0;
        }

        public Object getGenericValue(Column column) throws IOException {
            if(column.getCapabilities().getType().equals(ValueType.FLOAT)){
                return column.getGenericColumn().getFloatSingleValueRow(currentNum);
            }else if(column.getCapabilities().getType().equals(ValueType.LONG)){
                return column.getGenericColumn().getLongSingleValueRow(currentNum);
            }else{
                throw new IOException("column type error");
            }
        }
        @Override
        public boolean next(NullWritable key, DruidStruct value) throws IOException {
            if(currentNum >= rowNum){
                return false;
            }
            for(int i = 0; i < this.colIds.length; i++){
                // 检测是否是　_time 列
                if(colNames[colIds[i]].equals(timeColName)){
                    value.setFields(
                            Integer.parseInt(new DateTime((long)getGenericValue(queryableIndex.getColumn("__time"))).toString("yyyyMMdd")),
                            colIds[i]);

                }else{
                    Column queryColumn = queryableIndex.getColumn(colNames[colIds[i]]);

                    if(queryColumn.getCapabilities().getType().equals(ValueType.LONG) ||
                            queryColumn.getCapabilities().getType().equals(ValueType.FLOAT)){
                        value.setFields(getGenericValue(queryColumn), colIds[i]);
                    }else{
                        int index = ((SimpleDictionaryEncodedColumn)queryColumn.getDictionaryEncoding()).getSingleValueRow(currentNum);
                        value.setFields(queryColumn.getBitmapIndex().getValue(index), colIds[i]);
                    }
                }
            }
            currentNum ++;

            return true;
        }

        @Override
        public NullWritable createKey() {
            return null;
        }

        @Override
        public DruidStruct createValue() {
            return druidStruct;
        }

        @Override
        public long getPos() throws IOException {
            return 0;
        }

        @Override
        public void close() throws IOException {
            for(File children : this.segmentFile.listFiles()){
                children.delete();
            }

            this.segmentFile.delete();
        }

        @Override
        public float getProgress() throws IOException {
            return (float)currentNum/rowNum;
        }
    }

    @Override
    public RecordReader<NullWritable, DruidStruct> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
        /**
         * 1. 下载file到本地目录
         * 2. 解析元数据
         * 3. 建立映射关系
         */
        DruidSplit druidSplit = (DruidSplit) split;
        // segment 路径
        Path path = new Path(druidSplit.getPath().toString()+ "/index.zip");
        // 下载到本地路径
        String localSegmentPath = job.get(HiveConf.ConfVars.HIVEHISTORYFILELOC.varname) + "/" + MD5Hash.digest(new DateTime().toString()).toString(); // 获取临时目录　: /tmp/${UserName}

        File fout = new File(localSegmentPath);
        if(!fout.mkdir()){
            throw new IOException("Can not create dir : " + fout.getPath());
        }

        try{
            FileSystem fs = druidSplit.getPath().getFileSystem(job);
            InputStream in = fs.open(path);
            DataInputStream fin = new DataInputStream(in);
            ZipInputStream zis = new ZipInputStream(fin);


            byte[] buffOut = new byte[1024];
            ZipEntry entry = null;
            while ((entry= zis.getNextEntry()) != null) {
                String entryName = entry.getName();

                DataOutputStream Dout = new DataOutputStream(new FileOutputStream(fout + "/" + entryName));

                int b = 0;
                while ((b = zis.read(buffOut)) != -1) {
                    Dout.write(buffOut, 0, b);
                }
                Dout.flush();
                Dout.close();
            }

        }catch (IOException e){
            for(File file : fout.listFiles()){
                file.delete();
            }
            fout.delete();
            throw new IOException("get File Error");
        }

        if(!fout.isDirectory()){
            throw new IOException("segment 文件下载失败");
        }

        return new DruidRecordReader(job, fout);
    }
}
