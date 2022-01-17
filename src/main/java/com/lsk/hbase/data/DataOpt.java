package com.lsk.hbase.data;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @Description
 * @Author sikang.liu
 * @Date 2022-01-17 19:44
 */
public class DataOpt {
    public static Configuration conf;

    static {
        //使用 HBaseConfiguration 的单例方法实例化
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.166.9.102");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
    }


    public static void main(String[] args) throws IOException {
        addRowData("asd", "01", "info", "name", "kwc");
        addRowData("asd", "01", "info", "age", "18");
        addRowData("asd", "01", "info2", "zhiye", "student");
        addRowData("asd", "01", "info2", "salar", "23000");

        deleteMultiRow("asd", "01", "info", "age");

        getAllRows("asd");

        getRow("asd", "01");

        getRowQualifier("asd", "01", "info", "name");

    }

    public static void addRowData(String tableName, String rowKey, String columnFamily, String column, String value)
            throws IOException {
        //创建 HTable 对象
        HTable hTable = new HTable(conf, tableName);
        //向表中插入数据
        Put put = new Put(Bytes.toBytes(rowKey));
        //向 Put 对象中组装数据
        put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        hTable.put(put);
        hTable.close();
        System.out.println("插入数据成功");
    }

    public static void deleteMultiRow(String tableName, String... rows) throws IOException {
        HTable hTable = new HTable(conf, tableName);
        List<Delete> deleteList = new ArrayList<Delete>();
        for (String row : rows) {
            Delete delete = new Delete(Bytes.toBytes(row));
            deleteList.add(delete);
        }
        hTable.delete(deleteList);
        hTable.close();
    }

    public static void getAllRows(String tableName) throws IOException {
        HTable hTable = new HTable(conf, tableName);
        //得到用于扫描 region 的对象
        Scan scan = new Scan();
        //使用 HTable 得到 resultcanner 实现类的对象
        ResultScanner resultScanner = hTable.getScanner(scan);
        for (Result result : resultScanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                //得到 rowkey
                System.out.println(" 行键 :" + Bytes.toString(CellUtil.cloneRow(cell)));
                //得到列族
                System.out.println(" 列族 " + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println(" 列 :" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println(" 值 :" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }

    public static void getRow(String tableName, String rowKey) throws IOException {
        HTable table = new HTable(conf, tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
        //get.setMaxVersions();显示所有版本
        //get.setTimeStamp();显示指定时间戳的版本
        Result result = table.get(get);
        for (Cell cell : result.rawCells()) {
            System.out.println(" 行 键 :" + Bytes.toString(result.getRow()));
            System.out.println(" 列 族 " + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println(" 列 :" + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println(" 值 :" + Bytes.toString(CellUtil.cloneValue(cell)));
            System.out.println("时间戳:" + cell.getTimestamp());
        }
    }

    public static void getRowQualifier(String tableName, String rowKey, String family, String
            qualifier) throws IOException {
        HTable table = new HTable(conf, tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        Result result = table.get(get);
        for (Cell cell : result.rawCells()) {
            System.out.println(" 行 键 :" + Bytes.toString(result.getRow()));
            System.out.println(" 列 族 " + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println(" 列 :" + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println(" 值 :" + Bytes.toString(CellUtil.cloneValue(cell)));
        }
    }

}
