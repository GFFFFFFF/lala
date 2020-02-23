package com.bw.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import com.bw.constants.Constants;

public class HBaseUtil {
		
	//创建命名空间
	public static void createNameSpace(String nameSpace) throws IOException{
		
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "192.168.100.129");
		//获取Connection对象
		Connection connection = ConnectionFactory.createConnection(conf);
		//获取Admin对象
		Admin admin = connection.getAdmin();
		//构建命名空间描述其
		NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();
		//创建命名空间
		admin.createNamespace(namespaceDescriptor);
		//关闭资源
		admin.close();
		connection.close();
	}
	
	
	
	//判断表是否存在
	public static boolean isTableExist(String tableName) throws IOException{
		
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "192.168.100.129");
		//获取Connection对象
		Connection connection = ConnectionFactory.createConnection(conf);
		//获取Admin对象
		Admin admin = connection.getAdmin();
		
		//判断是否存在
		return admin.tableExists(TableName.valueOf(tableName));
		
	}
	
	
	//创建表
	@SuppressWarnings("deprecation")
	public static void createTable(String tableName,int version,String... cfs) throws IOException{
		
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "192.168.100.129");
		
		//判断是否传入了列族信息
		if(cfs.length<=0){
			System.out.println("请设置列族信息");
		}
		//判断表是否存在
		if(isTableExist(tableName)){
			System.out.println(tableName+"表已经存在");
		}else{
			//获取Connection对象
			Connection connection = ConnectionFactory.createConnection(conf);
			//获取Admin对象
			Admin admin = connection.getAdmin();
			//创建表描述其
			HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
			//循环添加列族信息
			for (String cf : cfs) {
				//获取列族描述器
				HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
				//设置版本
				hColumnDescriptor.setMaxVersions(version);
				//添加列族信息
				hTableDescriptor.addFamily(hColumnDescriptor);
			}
			//创建表
			admin.createTable(hTableDescriptor);
			//关闭资源
			admin.close();
			connection.close();
		}
		
		
	}
	

}
