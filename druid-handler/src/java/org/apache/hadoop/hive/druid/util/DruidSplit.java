package org.apache.hadoop.hive.druid.util;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.FileSplit;

import java.io.Serializable;

/**
 * <pre>
 * </pre>
 *
 * @author saligia
 * @date 18-1-25
 */
public class DruidSplit extends FileSplit implements InputSplit{
    public ExprNodeGenericFuncDesc pushedPredicateObject;

    public DruidSplit(){
        super(null, 0, 0, (String[]) null);
    }
    public void init(){

    }
    public DruidSplit(Path file, long start, long length, String[] hosts) {
        super(file, start, length, hosts);
    }

    public ExprNodeGenericFuncDesc getDecomposedPredicate() {
        return pushedPredicateObject;
    }

    public void setDecomposedPredicate(ExprNodeGenericFuncDesc pushedPredicateObject) {
        this.pushedPredicateObject = pushedPredicateObject;
    }
}
