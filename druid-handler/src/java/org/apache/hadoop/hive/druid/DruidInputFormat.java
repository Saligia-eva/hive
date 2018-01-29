package org.apache.hadoop.hive.druid;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.druid.struct.DruidSplit;
import org.apache.hadoop.hive.druid.struct.DruidStruct;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler.DecomposedPredicate;
import org.apache.hadoop.hive.ql.plan.*;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeParser;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * <pre>
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
     * @author
     * @param job 作业相关配置
     * @return
     */
    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {

        // 得到过滤器
        DecomposedPredicate decomposedPredicate = getDecomposePredicate(job);
        // 获取输入路径
        Path[] paths = getInputPaths(job, decomposedPredicate);

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

        // 解析日期聚合
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
    public static Path[] getInputPaths(JobConf conf, DecomposedPredicate decomposedPredicate) throws IOException {

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
                DateTime dateTimeStart = DateTime.parse(timePath[0]);
                DateTime dateTimeStop = DateTime.parse(timePath[1]);

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

        return (Path[]) bucketSegment.toArray();
    }

    public static ExprNodeDesc getTimeFilter(DateTime startTime, DateTime stopTime, ExprNodeDesc funcDesc){

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
            int timeStartInt = Integer.parseInt(startTime.toString("yyyyMMdd"));
            int timeStopInt = Integer.parseInt(stopTime.toString("yyyyMMdd"));

            int timeGetInt = (int)((ExprNodeConstantDesc)funcDesc.getChildren().get(1)).getValue();

            BooleanWritable bool1 = (BooleanWritable)((ExprNodeGenericFuncDesc) funcDesc).getGenericUDF().evaluate(timeStartInt, timeGetInt);
            BooleanWritable bool2 = (BooleanWritable)((ExprNodeGenericFuncDesc) funcDesc).getGenericUDF().evaluate(timeStopInt, timeGetInt);
            BooleanWritable bool3 = new BooleanWritable(timeStartInt <= timeGetInt && timeGetInt < timeStopInt);

            return new ExprNodeConstantDesc(TypeInfoFactory.booleanTypeInfo,
                    bool1.get() || bool2.get() || bool3.get());
        }

        BooleanWritable boolres = (BooleanWritable)(((ExprNodeGenericFuncDesc) funcDesc).getGenericUDF().evaluate(funcDesc.getChildren().get(0),funcDesc.getChildren().get(1)));
        return new ExprNodeConstantDesc(TypeInfoFactory.booleanTypeInfo, boolres.get());
    }

    private static class  DruidRecordReader implements RecordReader<NullWritable, DruidStruct> {

        @Override
        public boolean next(NullWritable key, DruidStruct value) throws IOException {
            return false;
        }

        @Override
        public NullWritable createKey() {
            return null;
        }

        @Override
        public DruidStruct createValue() {
            return null;
        }

        @Override
        public long getPos() throws IOException {
            return 0;
        }

        @Override
        public void close() throws IOException {

        }

        @Override
        public float getProgress() throws IOException {
            return 0;
        }
    }


    @Override
    public RecordReader<NullWritable, DruidStruct> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {

        return null;
    }
}
