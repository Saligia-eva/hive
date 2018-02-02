package org.apache.hadoop.hive.druid;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.druid.util.DruidSegment;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;

/**
 * <pre>
 * </pre>
 *
 * @author saligia
 * @date 18-1-23
 */
public class DruidOutputFormat implements OutputFormat<NullWritable, DruidSegment>{
    @Override
    public RecordWriter<NullWritable, DruidSegment> getRecordWriter(FileSystem ignored, JobConf job, String name, Progressable progress) throws IOException {
        return null;
    }

    @Override
    public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {

    }
}
