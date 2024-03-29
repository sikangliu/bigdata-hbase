package com.lsk.hbase1.weibo.utils;

import com.lsk.hbase1.weibo.contants.Constants;
import java.io.IOException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class HBaseUtil {

    /**
     * 创建名称空间
     */
    public static void createNameSpace(String nameSpace) throws IOException {
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        Admin admin = connection.getAdmin();
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();
        admin.createNamespace(namespaceDescriptor);
        admin.close();
        connection.close();
    }

    /**
     * 判断表是否存在
     */
    private static boolean isTableExist(String tableName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        Admin admin = connection.getAdmin();
        boolean exists = admin.tableExists(TableName.valueOf(tableName));
        admin.close();
        connection.close();
        return exists;
    }

    /**
     * 创建表
     */
    public static void createTable(String tableName, int versions, String... cfs) throws IOException {
        if (cfs.length <= 0) {
            System.out.println("请设置列族信息！！！");
            return;
        }
        if (isTableExist(tableName)) {
            System.out.println(tableName + "表已存在！");
        }
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        Admin admin = connection.getAdmin();
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for (String cf : cfs) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            hColumnDescriptor.setMaxVersions(versions);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
        admin.createTable(hTableDescriptor);
        admin.close();
        connection.close();
    }
}
