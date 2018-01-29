package org.apache.hadoop.hive.druid.struct;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler.DecomposedPredicate;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.FileSplit;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * <pre>
 * </pre>
 *
 * @author saligia
 * @date 18-1-25
 */
public class DruidSplit extends FileSplit implements InputSplit{
    public Serializable pushedPredicateObject;
    public DruidSplit(Path file, long start, long length, String[] hosts) {
        super(file, start, length, hosts);
    }

    public Serializable getDecomposedPredicate() {
        return pushedPredicateObject;
    }

    public void setDecomposedPredicate(Serializable pushedPredicateObject) {
        this.pushedPredicateObject = pushedPredicateObject;
    }
}
