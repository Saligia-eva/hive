package org.apache.hadoop.hive.druid;

import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;

/**
 * <pre>
 *     Druid 数据解析工具
 *     直接获取 Druid 底层存储格式的文件， 并解析输出
 * </pre>
 *
 * @author saligia
 * @date 18-1-23
 */
public class DruidStorageHandler extends DefaultStorageHandler
        implements HiveMetaHook, HiveStoragePredicateHandler {

    /**
     * <pre>
     *    预创建表的准备操作
     * </pre>
     *
     * @author saligia
     * @param table
     * @return null
     */
    @Override
    public void preCreateTable(Table table) throws MetaException {

    }

    /**
     * <pre>
     *     创建表失败时候的回滚操作
     * </pre>
     *
     * @author saligia
     * @param table
     * @return null
     */
    @Override
    public void rollbackCreateTable(Table table) throws MetaException {

    }

    /**
     * <pre>
     *     创建表完成之后的善后操作
     * </pre>
     *
     * @author saligia
     * @param table
     * @return null
     */
    @Override
    public void commitCreateTable(Table table) throws MetaException {

    }

    /**
     * <pre>
     *     预删除表的准备操作
     * </pre>
     *
     * @author saligia
     * @param table
     * @return null
     */
    @Override
    public void preDropTable(Table table) throws MetaException {

    }

    /**
     * <pre>
     *     删除表失败时候的回滚操作
     * </pre>
     *
     * @author saligia
     * @param table
     * @return null
     */
    @Override
    public void rollbackDropTable(Table table) throws MetaException {

    }

    /**
     * <pre>
     *     删除表时候的善后操作
     * </pre>
     *
     * @author saligia
     * @param  table : 表信息
     * @param deleteData: 是否删除数据
     * @return null
     */
    @Override
    public void commitDropTable(Table table, boolean deleteData) throws MetaException {

    }


    /**
     * <pre>
     *     得到 Druid 的数据输入数据类
     * </pre>
     *
     * @author　saligia
     * @param
     * @return DruidInputFormat
     */
    @Override
    public Class<? extends InputFormat> getInputFormatClass() {
        return DruidInputFormat.class;
    }

    @Override
    public Class<? extends OutputFormat> getOutputFormatClass() {
        return DruidOutputFormat.class;
    }

    @Override
    public Class<? extends SerDe> getSerDeClass() {
        return super.getSerDeClass();
    }

    @Override
    public HiveMetaHook getMetaHook() {
        return this;
    }

    /**
     * <pre>
     *     谓词下推工具
     * </pre>
     *
     * @author　saligia
     * @param jobConf 作业提交的配置
     * @param deserializer
     * @param predicate  谓词信息
     * @return　谓词下推信息
     */
    @Override
    public DecomposedPredicate decomposePredicate(JobConf jobConf, Deserializer deserializer, ExprNodeDesc predicate) {
        return null;
    }
}
