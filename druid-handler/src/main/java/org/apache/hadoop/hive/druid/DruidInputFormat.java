package org.apache.hadoop.hive.druid;

import org.apache.hadoop.hive.druid.struct.DruidStruct;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

/**
 * <pre>
 * </pre>
 *
 * @author saligia
 * @date 18-1-23
 */
public class DruidInputFormat implements InputFormat<NullWritable, DruidStruct> {
    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
        return new InputSplit[0];
    }

    @Override
    public RecordReader<NullWritable, DruidStruct> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
        return null;
    }
}
