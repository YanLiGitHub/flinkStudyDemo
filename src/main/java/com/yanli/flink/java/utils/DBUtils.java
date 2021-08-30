package com.yanli.flink.java.utils;


import com.yanli.flink.java.config.MysqlConfig;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author YanLi
 * @version 1.0
 * @ClassName: DBUtils
 * @date 2021/7/13 1:39 下午
 */
public class DBUtils {

    public static Connection getConn(){
        Connection conn = null;
        try {
            Class.forName(MysqlConfig.DRIVER_CLASS);
            conn = DriverManager.getConnection(MysqlConfig.DB_URL,MysqlConfig.USER_NAME,MysqlConfig.PASSWORD);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }

        return conn;
    }

    /*
     * 将rs结果转换成对象列表
     * @param rs jdbc结果集
     * @param clazz 对象的映射类
     * return 封装了对象的结果列表
     */
    public static List populate(ResultSet rs , Class clazz) throws SQLException, InstantiationException, IllegalAccessException{
        //结果集的元素对象
        ResultSetMetaData rsmd = rs.getMetaData();
        //获取结果集的元素个数
        int colCount = rsmd.getColumnCount();

        //返回结果的列表集合
        List list = new ArrayList();
        //业务对象的属性数组
        Field[] fields = clazz.getDeclaredFields();
        while(rs.next()){//对每一条记录进行操作
            Object obj = clazz.newInstance();//构造业务对象实体
            //将每一个字段取出进行赋值
            for(int i = 1;i<=colCount;i++){
                Object value = rs.getObject(i);
                //寻找该列对应的对象属性
                for (Field f : fields) {
                    //如果匹配进行赋值
                    if (f.getName().equalsIgnoreCase(rsmd.getColumnName(i))) {
                        boolean flag = f.isAccessible();
                        f.setAccessible(true);
                        f.set(obj, value);
                        f.setAccessible(flag);
                    }
                }
            }
            list.add(obj);
        }
        return list;
    }
}
