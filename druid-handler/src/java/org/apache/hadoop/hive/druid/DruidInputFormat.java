package org.apache.hadoop.hive.druid;

import com.fasterxml.jackson.core.JsonParser;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;
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
import org.apache.hadoop.hive.druid.util.DruidSplit;
import org.apache.hadoop.hive.druid.util.DruidSegment;
import org.apache.hadoop.hive.druid.util.SegmentDesc;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler.DecomposedPredicate;
import org.apache.hadoop.hive.ql.plan.*;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.*;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import com.fasterxml.jackson.databind.JsonNode;
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
public class DruidInputFormat implements InputFormat<NullWritable, DruidSegment> {

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

        Path[] paths = null;
        try {
            paths = getInputPaths(job);
        } catch (HiveException e) {
            throw new IOException(e);
        }

        if(paths == null){
            return null;
        }

        DruidSplit[] intpuSplit = new DruidSplit[paths.length];

        for(int i = 0 ; i < intpuSplit.length; i++){
            intpuSplit[i] = new DruidSplit(paths[i], 0, 0, null);
            intpuSplit[i].put(SegmentDesc.DRUID_TIMESTAMP_COL, job.get(SegmentDesc.DRUID_TIMESTAMP_COL));
        }

        return intpuSplit;
    }


    /**
     * <pre>
     *     根据谓词信息拿到时间戳
     *     此处代码需要优化
     * </pre>
     *
     * @author　saligia
     * @param conf 任务配置信息
     * @return　path 路径
     */
    public Path[] getInputPaths(JobConf conf) throws IOException, HiveException {

        //.../${TableName}/${StartTime}_${EndTime}/${TimeVersion}/${File}
        // 得到时间过滤
        ExprNodeGenericFuncDesc filterExpr = null;
        String filterExprSerialized = conf.get(TableScanDesc.FILTER_EXPR_CONF_STR);
        if(filterExprSerialized != null){
            filterExpr = Utilities.deserializeExpression(filterExprSerialized);
        }
        // 获取输入路径
        String tablePath = conf.get(SegmentDesc.HIVE_INPUT_DIR, "");
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

            // 得到时间过滤
            if (filterExpr != null) {

                String []timePath = timeFileStatus.getPath().getName().split("_");
                DateTime dateTimeStart;
                DateTime dateTimeStop;
                if(!timeFileStatus.getPath().getName().contains(":")){
                    dateTimeStart = DateTime.parse(timePath[0], DateTimeFormat.forPattern("yyyyMMdd'T'HHmmss.SSSZ"));
                    dateTimeStop = DateTime.parse(timePath[1], DateTimeFormat.forPattern("yyyyMMdd'T'HHmmss.SSSZ"));
                }else{
                    dateTimeStart = DateTime.parse(timePath[0], DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ"));
                    dateTimeStop = DateTime.parse(timePath[1], DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ"));
                }
                if(!(boolean)((ExprNodeConstantDesc)getTimeFilter(dateTimeStart, dateTimeStop, filterExpr)).getValue()){
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

    public BooleanWritable compareTo(int obj1, int obj2, String op) throws IOException {
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
        }else if("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual".equals(obj2)){
            return new BooleanWritable(obj1 != obj2);
        }else{
            throw new IOException("Error Operator");
        }
    }

    public BooleanWritable compareTo(boolean obj1, boolean obj2, String op) throws IOException  {
        if("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr".equals(op)){
            return new BooleanWritable(obj1 || obj2);
        }else if("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd".equals(op)){
            return new BooleanWritable(obj1 && obj2);
        }else{
            throw new IOException("Error Operator");
        }
    }

    public ExprNodeDesc getTimeFilter(DateTime startTime, DateTime stopTime, ExprNodeDesc funcDesc) throws HiveException, IOException {

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

    private static class DruidRecordReader implements RecordReader<NullWritable, DruidSegment> {
        private QueryableIndex queryableIndex = null;                                     // 获取 segment 的映射接口
        private DruidSegment druidSegment = null;                                         // 数据输出接口
        private String timeColName ;                                                      // 时间列标记
        private int[] colIds;                                                             //
        private File segmentFile;                                                          // segment文件标记
        private int rowNum;                                                                // 记录总的行数量
        private int currentNum;                                                            // 当前读取的行数
        private ImmutableBitmap bitFileter ;                                               // 过滤器工具

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

            this.segmentFile  = file;
            String[] colStr = conf.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR).split(",");
            colIds = new int[colStr.length];
            for(int i = 0 ; i < colIds.length; i++){
                colIds[i] = Integer.parseInt(colStr[i]);
            }
            this.timeColName = conf.get(SegmentDesc.DRUID_TIMESTAMP_COL);

            String[] colNames = conf.get(serdeConstants.LIST_COLUMNS).split(",");
            String [] colTypes = conf.get(serdeConstants.LIST_COLUMN_TYPES).split(",");

            this.druidSegment = new DruidSegment(colNames.length, colNames, colTypes);

            String filterObjectSerialized = conf.get(TableScanDesc.FILTER_OBJECT_CONF_STR);
            if(filterObjectSerialized != null){
                ExprNodeGenericFuncDesc serializable = Utilities.deserializeExpression(filterObjectSerialized);
                this.bitFileter = getDimensionFileter(serializable);;
            }

            this.rowNum = queryableIndex.getNumRows();
            this.currentNum = 0;
        }


        /**
         * <pre>
         *     维度过滤器
         * </pre>
         *
         * @author saligia
         * @param　funcDesc 谓词过滤信息
         * @return　bit 形式
         */
        private ImmutableBitmap getDimensionFileter(ExprNodeGenericFuncDesc funcDesc) throws IOException {

            GenericUDF udf = funcDesc.getGenericUDF();

            if ("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd".equals(udf.getUdfName())){  // and
                ImmutableBitmap child1 = getDimensionFileter((ExprNodeGenericFuncDesc)funcDesc.getChildren().get(0));
                ImmutableBitmap child2 = getDimensionFileter((ExprNodeGenericFuncDesc)funcDesc.getChildren().get(1));

                return child1.intersection(child2);

            }else if("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr".equals(udf.getUdfName())){ // or
                ImmutableBitmap child1 = getDimensionFileter((ExprNodeGenericFuncDesc)funcDesc.getChildren().get(0));
                ImmutableBitmap child2 = getDimensionFileter((ExprNodeGenericFuncDesc)funcDesc.getChildren().get(1));

                return child1.union(child2);
            }else if (funcDesc.getChildren().get(0) instanceof ExprNodeColumnDesc &&
                    ("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual".equals(udf.getUdfName()) ||
                    "org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual".equals(udf.getUdfName()))){

                String colname = ((ExprNodeColumnDesc)funcDesc.getChildren().get(0)).getColumn();
                String coltype = druidSegment.getFieldTypeByName(colname);

                if(coltype.equals("string")){
                    // 对于维度的处理
                    String colvalue = (String)((ExprNodeConstantDesc)funcDesc.getChildren().get(1)).getValue();

                    if(!queryableIndex.getColumn(colname).getCapabilities().getType().equals(ValueType.STRING)){
                        throw new IOException("column : " + colname + " type error");
                    }

                    int index = queryableIndex.getColumn(colname).getBitmapIndex().getIndex(colvalue);
                    ImmutableBitmap bitmap = null;

                    if(index == SegmentDesc.NULL_INDEX){ // 查找到一个无效的值
                        MutableBitmap emptyBitMap  = queryableIndex.getBitmapFactoryForDimensions().makeEmptyMutableBitmap();
                        emptyBitMap.add(queryableIndex.getNumRows());
                        bitmap = queryableIndex.getBitmapFactoryForDimensions().makeImmutableBitmap(emptyBitMap);
                    }else{
                        bitmap = queryableIndex.getColumn(colname).getBitmapIndex().getBitmap(index);
                    }

                    if("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual".equals(udf.getUdfName())){
                        return bitmap;
                    }else{
                        return queryableIndex.getBitmapFactoryForDimensions().complement(bitmap);
                    }
                    // 非维度的处理
                }else{
                    MutableBitmap emptyBitMap  = queryableIndex.getBitmapFactoryForDimensions().makeEmptyMutableBitmap();
                    emptyBitMap.add(queryableIndex.getNumRows());
                    ImmutableBitmap bitmap = queryableIndex.getBitmapFactoryForDimensions().makeImmutableBitmap(emptyBitMap);
                    return queryableIndex.getBitmapFactoryForDimensions().complement(bitmap);
                }

            }

            // 其他类型的处理
            MutableBitmap emptyBitMap  = queryableIndex.getBitmapFactoryForDimensions().makeEmptyMutableBitmap();
            emptyBitMap.add(queryableIndex.getNumRows());
            ImmutableBitmap bitmap = queryableIndex.getBitmapFactoryForDimensions().makeImmutableBitmap(emptyBitMap);
            return queryableIndex.getBitmapFactoryForDimensions().complement(bitmap);

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
        public boolean next(NullWritable key, DruidSegment value) throws IOException {
            if(bitFileter != null ){
                while(!bitFileter.get(currentNum) && currentNum < rowNum){
                    currentNum ++;
                }
            }
            if(currentNum >= rowNum){
                return false;
            }
            for(int i = 0; i < this.colIds.length; i++){
                // 检测是否是　_time 列
                if(druidSegment.getFileldName(colIds[i]).equals(timeColName)){
                    value.setFields(
                            Integer.parseInt(new DateTime((long)getGenericValue(queryableIndex.getColumn("__time"))).toString("yyyyMMdd")),
                            colIds[i]);

                }else{
                    Column queryColumn = queryableIndex.getColumn(druidSegment.getFileldName(colIds[i]));

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
        public DruidSegment createValue() {
            return druidSegment;
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

    /**
     * <pre>
     *     检查数据类型是否正确
     *     这里检查的是表的所有有效列，不单单是查询的列
     * </pre>
     *
     * @author　saligia
     * @param split segment 文件
     * @param job　记录查询的列的相关信息
     * @return null　如果错误则抛出异常返回
     */
    private void checkSegmentDesc (InputSplit split, JobConf job) throws IOException {

        DruidSplit fileSplit = (DruidSplit) split;

        for(String key : fileSplit.getKeys()){
            job.set(key, fileSplit.get(key));
        }

        Path path = new Path(fileSplit.getPath().toString() + SegmentDesc.SEGMENT_DESC);
        FileSystem fs = fileSplit.getPath().getFileSystem(job);

        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = new JsonFactory();

        StringBuffer buffer = new StringBuffer();
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));

        String strLen;
        while((strLen = reader.readLine()) !=  null){
            buffer.append(strLen);
        }

        reader.close();

        JsonParser parser = factory.createJsonParser(buffer.toString());
        JsonNode descNode = mapper.readTree(parser);
        SegmentDesc segmentDesc = new SegmentDesc(descNode);

        // 检查用户的字段是否存在，　检查格式是否正确
        String[] columns = job.get(serdeConstants.LIST_COLUMNS).split(",");
        String[] columnType = job.get(serdeConstants.LIST_COLUMN_TYPES).split(",");

        // 剔除不属于表模式的列
        for(int i = 0 ; i < columns.length-3; i++){
            if(columns[i].equals(job.get(SegmentDesc.DRUID_TIMESTAMP_COL))){
                if(!columnType[i].equals("int")){
                    throw new IOException("Segment time colmn type error");
                }
            }else{
                SegmentDesc.ColumnType type = segmentDesc.checkColumn(columns[i]);

                if(type.equals(SegmentDesc.ColumnType.DIMENSSION) && columnType[i].equals("string") ||
                        type.equals(SegmentDesc.ColumnType.METRIC) && (columnType[i].equals("float") || columnType[i].equals("bigint"))){
                    continue;
                }else{
                    throw new IOException(columns[i] + ":" + columnType[i] + "Column error");
                }
            }
        }
    }

    /**
     * <pre>
     *     下载Segment文件并解压到本地临时文件:/tmp/${HashCode}
     * </pre>
     *
     * @author saligia
     * @param split segment 文件信息
     * @param job 作业相关信息，这里使用的文件系统配置FileSystem
     * @param localDir 下载的本地文件路径
     * @return null 如果错误则抛出异常返回
     */
    public void getSegmentFile(InputSplit split, JobConf job, File localDir) throws IOException {
        FileSplit segmentSplit  = (FileSplit) split;
        Path segmentPath = new Path(segmentSplit.getPath().toString()+ SegmentDesc.SEGMENT_FILE);
        FileSystem fs = segmentSplit.getPath().getFileSystem(job);

        InputStream in = fs.open(segmentPath);
        DataInputStream fin = new DataInputStream(in);
        ZipInputStream zis = new ZipInputStream(fin);

        byte[] buffOut = new byte[1024];
        ZipEntry entry = null;

        while ((entry= zis.getNextEntry()) != null) {
            String entryName = entry.getName();
            DataOutputStream Dout = new DataOutputStream(new FileOutputStream(localDir.getPath() + "/" + entryName));

            int b = 0;
            while ((b = zis.read(buffOut)) != -1) {
                Dout.write(buffOut, 0, b);
            }

            Dout.flush();
            Dout.close();
        }
        zis.close();
        in.close();
    }


    /**
     * <pre>
     *    根据分区信息来获取指定分区文件的读取方式
     *    先将指定分区的segment 信息解析下来，校验读取的字段是否存在并且格式是否正确。
     *    然后将文件下载到本地文件系统中，建立MMP的映射关系
     *    然后返回对应的RecordReader信息
     * </pre>
     *
     * @author saligia
     * @param split 分区信息
     * @param job 作业配置属性信息
     * @return　DruidRedordReader 分区文件的读取方式
     */
    @Override
    public RecordReader<NullWritable, DruidSegment> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {

        checkSegmentDesc(split, job);

        // 下载文件到本地临时文件　: /tmp/${UserName}
        String localSegmentPath = job.get(HiveConf.ConfVars.HIVEHISTORYFILELOC.varname) + "/" + MD5Hash.digest(new DateTime().toString()).toString();
        File fout = new File(localSegmentPath);
        if(!fout.mkdir()){
            throw new IOException("Can not create dir : " + fout.getPath());
        }
        try{
            getSegmentFile(split, job, fout);
        }catch (IOException e){
            for(File file : fout.listFiles()){
                file.delete();
            }
            fout.delete();
            throw new IOException("Get File Segment Error");
        }

        return new DruidRecordReader(job, fout);
    }
}
