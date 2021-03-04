package com.yanli.flink.java.streamingApi.hbase;

import com.alibaba.fastjson.JSON;

import com.yanli.flink.java.config.HBaseConfig;
import com.yanli.flink.java.pojo.hbase.ZKWisdomAnswerDetial;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author YanLi
 * @version 1.0
 * @ClassName: HbaseReader
 * @date 2021/2/24 5:11 下午
 * flink 连接 hbase source
 */
public class HbaseReader<T> extends RichSourceFunction<ZKWisdomAnswerDetial> {

    private static final Logger logger = LoggerFactory.getLogger(HbaseReader.class);


    //hbase 默认连接参数
    private static Connection conn;
    private static Table table;
    private static Scan scan;
    private static List<Get> getList = new ArrayList<>();

    /**
     * 表名
     */
    private String tableName;
    /**
     * 列簇
     */
    private String cf;

    /**
     * rowkey
     */
    private String rowKey;

    /**
     * rowkey
     */
    private List<String> rowKeyList;
    /**
     * rowkey前缀
     */
    private String prefix;

    /**
     * 正则表达式
     */
    private String regex;

    /**
     * hbase 查询结果序列化实体类
     */

    private Class<T> entityClass;

    private String zookeeperQuorum;

    /**
     * rowkey前缀匹配查询
     *
     * @param tableName
     * @param cf
     * @param rowKeyPrefix
     */
    public HbaseReader(String tableName, String cf,String zookeeperQuorum, String rowKeyPrefix) {
        this(tableName,cf,zookeeperQuorum,rowKeyPrefix,null);
    }

    /**
     * 全表扫描
     *
     * @param tableName
     * @param cf
     */
    public HbaseReader(String tableName, String cf,String zookeeperQuorum) {
        this(tableName, cf,zookeeperQuorum, null,null);
    }

    /**
     * 单rowkey 精准查询
     *
     * @param tableName
     * @param cf
     * @param rowKey
     */
//    public HbaseReader(String tableName, String cf,String zookeeperQuorum, String rowKey) {
//        this.tableName = tableName;
//        this.cf = cf;
//        this.rowKey = rowKey;
//        this.zookeeperQuorum=zookeeperQuorum;
//        Type type = this.getClass().getGenericSuperclass(); // generic 泛型
//        if(type instanceof ParameterizedType){
//            // 强制转化“参数化类型”
//            ParameterizedType parameterizedType = (ParameterizedType) type;
//            // 参数化类型中可能有多个泛型参数
//            Type[] types = parameterizedType.getActualTypeArguments();
//            // 获取数据的第一个元素(User.class)
//            this.entityClass = (Class<T>) types[0]; // com.oa.shore.entity.User.class
//        }
//    }

    /**
     * 多个 rowkey 精准查询
     *
     * @param tableName
     * @param cf
     * @param rowKeyList
     */
    public HbaseReader(String tableName, String cf,String zookeeperQuorum, List rowKeyList) {
        this.tableName = tableName;
        this.cf = cf;
        this.zookeeperQuorum=zookeeperQuorum;
        this.rowKeyList = rowKeyList;
        Type type = this.getClass().getGenericSuperclass(); // generic 泛型
        if(type instanceof ParameterizedType){
            // 强制转化“参数化类型”
            ParameterizedType parameterizedType = (ParameterizedType) type;
            // 参数化类型中可能有多个泛型参数
            Type[] types = parameterizedType.getActualTypeArguments();
            // 获取数据的第一个元素(User.class)
            this.entityClass = (Class<T>) types[0]; // com.oa.shore.entity.User.class
        }
    }

    /**
     * rowkey前缀 正则匹配 查询
     *
     * @param tableName
     * @param cf
     * @param rowKeyPrefix
     * @param regex
     */
    public HbaseReader(String tableName, String cf,String zookeeperQuorum, String rowKeyPrefix, String regex) {
        this.tableName = tableName;
        this.cf = cf;
        this.zookeeperQuorum = zookeeperQuorum;
        this.prefix = rowKeyPrefix;
        this.regex = regex;
        Type type = this.getClass().getGenericSuperclass(); // generic 泛型
        if(type instanceof ParameterizedType){
            // 强制转化“参数化类型”
            ParameterizedType parameterizedType = (ParameterizedType) type;
            // 参数化类型中可能有多个泛型参数
            Type[] types = parameterizedType.getActualTypeArguments();
            // 获取数据的第一个元素(User.class)
            this.entityClass = (Class<T>) types[0]; // com.oa.shore.entity.User.class
        }
    }

    /**
     * 功能描述:
     * 〈连接hbase客户端〉
     *
     * @Param: 客户端连接配置
     * @Return: void
     * @Author: yanli
     * @Date: 2021/2/24 5:11 下午
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        conn = ConnectionFactory.createConnection(HBaseConfig.getHBaseConfig(zookeeperQuorum));
        table = conn.getTable(TableName.valueOf(tableName));
        scan = new Scan();
        if (rowKey != null && !rowKey.isEmpty()) {
            getList.add(new Get(this.rowKey.getBytes(StandardCharsets.UTF_8)));
        }
        if (rowKeyList != null && !rowKeyList.isEmpty()) {
            getList = rowKeyList.stream().map(x -> new Get(x.getBytes(StandardCharsets.UTF_8))).collect(Collectors.toList());
        }
        scan.addFamily(this.cf.getBytes(StandardCharsets.UTF_8));
        if (this.regex != null) {
            scan.setFilter(new RowFilter(CompareOperator.EQUAL, new RegexStringComparator(regex)));
        }
        if (this.prefix != null) {
            scan.setRowPrefixFilter(this.prefix.getBytes(StandardCharsets.UTF_8));
        }

    }

    /**
     * 功能描述:
     * 〈通过scan方法扫描hbase表，返回查询到的结果〉
     *
     * @Param: [sourceContext]
     * @Return: void
     * @Author: yanli
     * @Date: 2021/2/24 5:11 下午
     */
    @Override
    public void run(SourceContext sourceContext) throws Exception {
//        Tuple2<String, T> resultTuple = new Tuple2<>();
        if (getList != null && !getList.isEmpty()) {
            Result[] results = table.get(getList);
            for (Result result : results) {
                T entity = this.getEntity(result);
//                resultTuple.setFields(Bytes.toString(result.getRow()), entity);
                sourceContext.collect(entity);
            }
        } else {
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                T entity = this.getEntity(result);
//                resultTuple.setFields(Bytes.toString(result.getRow()), entity);
                sourceContext.collect(entity);
            }
        }

    }


    @Override
    public void cancel() {

    }

    @Override
    public void close() {
        try {
            if (table != null) {
                table.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("Close HBase Exception:", e.toString());
        }
    }

    public T getEntity(Result result) {
        try {
            T entity = entityClass.newInstance();
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                String col = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                try {
                    Method getCol = entityClass.getMethod(getGetMethodName(col));
                    Class<?> typeClazz = getCol.getReturnType();
                    Method m = entityClass.getMethod(getSetMethodName(col), typeClazz);

                    if (typeClazz == String.class) {
                        m.invoke(entity, Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                    } else if (typeClazz == Integer.class) {
                        m.invoke(entity, Bytes.toInt(cell.getValueArray(), cell.getValueOffset()));
                    } else if (typeClazz == Double.class) {
                        m.invoke(entity, Bytes.toDouble(cell.getValueArray(), cell.getValueOffset()));
                    } else if (typeClazz == List.class) {
                        m.invoke(entity, JSON.parseArray(Bytes.toString(cell.getValueArray(), cell.getValueOffset())));
                    }
                }
                catch (NoSuchMethodException e) {
                    System.out.println("没有找到" + col + "对应的set方法");
                } catch (Exception e) {
                    System.out.println(col);
                    System.out.println(cell);
                    e.printStackTrace();
                }
            }
//            getEntityHook(entity);
            return entity;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }


    private String getSetMethodName(String fieldName) {
        fieldName = fieldName.substring(0, 1).toUpperCase().concat(fieldName.substring(1));
        return "set" + fieldName;
    }

    private String getGetMethodName(String fieldName) {
        fieldName = fieldName.substring(0, 1).toUpperCase().concat(fieldName.substring(1));
        return "get" + fieldName;
    }

}
