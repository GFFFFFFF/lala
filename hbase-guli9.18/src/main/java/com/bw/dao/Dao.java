package com.bw.dao;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.bw.constants.Constants;

public class Dao {
	
	private static Configuration conf;
	private static Connection connection;
	
	static {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "192.168.100.129");
		try {
			connection = ConnectionFactory.createConnection(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	
	//发布微博
	public static void publishWeiBo(String uid,String content) throws IOException{
		/*Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "192.168.100.129");
		//获取Connection对象
		Connection connection = ConnectionFactory.createConnection(conf);*/
		
		//内容表
		//获取微博内容表对象
		Table contTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));
		//获取当前时间戳
		long timeMillis = System.currentTimeMillis();
		//获取RoeKey
		String rowKey = uid + "_" + timeMillis;
		//创建put对象
		Put contPut = new Put(Bytes.toBytes(rowKey));
		//给put赋值
		contPut.addColumn(Bytes.toBytes(Constants.CONTENT_TABLE_CF), Bytes.toBytes("content"), 
				Bytes.toBytes(content));
		//插入数据
		contTable.put(contPut);
		
		//收件箱表
		//获取用户关系表对象
		Table relaTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));
		//获取当前发布微博人的fans列族
		Get get = new Get(Bytes.toBytes(uid));
		get.addFamily(Bytes.toBytes(Constants.RELATION_TABLE_CF2));
		Result result = relaTable.get(get);
		//创建一个集合，存放内容表的put对象
		ArrayList<Put> inboxPuts = new ArrayList<>();
		//遍历粉丝
		for (Cell cell : result.rawCells()) {
			//构建收件箱表的put
			Put inboxPut = new Put(CellUtil.cloneQualifier(cell));
			//给收件箱表的put对象赋值
			inboxPut.addColumn(Bytes.toBytes(Constants.INBOX_TABLE_CF), Bytes.toBytes(uid),
					 Bytes.toBytes(rowKey));
			//将收件箱表的put对象存入集合
			inboxPuts.add(inboxPut);
		}
		//判断是否有粉丝
		if(inboxPuts.size()>0){
			//获取收件箱表
			Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));
			//收件箱表插入
			inboxTable.put(inboxPuts);
			inboxTable.close();
		}else{
			System.out.println("你木得粉丝");
		}
		relaTable.close();
		contTable.close();
		connection.close();
	}
	
	
	
	//关注用户
	@SuppressWarnings("deprecation")
	public static void addAttends(String uid,String... attends) throws IOException{
		//校验是否添加了待关注的人
		if(attends.length<=0){
			System.out.println("选择待关注的人");
			return ;
		}
		//操作用户关系表
		//获取用户关系表对象
		Table relaTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));
		//建一个集合，用于存放关系表的put
		ArrayList<Put> relaPuts = new ArrayList<>();
		//创建操作者的put对象
		Put uidPut = new Put(Bytes.toBytes(uid));
		//循环创建被关注者的put对象
		for (String attend : attends) {
			//给操作者put对象赋值
			uidPut.addColumn(Bytes.toBytes(Constants.RELATION_TABLE_CF1), 
					Bytes.toBytes(attend), Bytes.toBytes(attend));
			//创建被关注人的put对象
			Put attendPut = new Put(Bytes.toBytes(attend));
			//给被关注人的put对象赋值
			attendPut.addColumn(Bytes.toBytes(Constants.RELATION_TABLE_CF2),
					Bytes.toBytes(uid), Bytes.toBytes(uid));
			//将被关注人的put对象放入集合
			relaPuts.add(attendPut);
		}
		//将操作者put放入集合
		relaPuts.add(uidPut);
		//用户关系表插入数据
		relaTable.put(relaPuts);
		
		//操作收件箱表
		//获取内容表对象
		Table contTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));
		//创建收件箱表的put对象
		Put inboxPut = new Put(Bytes.toBytes(uid));
		//循环attends 获取每个被关注人的近期发布微博
		for (String attend : attends) {
			//获取当前被关注者的近期发布的微博
			Scan scan = new Scan(Bytes.toBytes(attend+"_"),Bytes.toBytes(attend+"|"));
			ResultScanner resultScanner = contTable.getScanner(scan);
			//对获取的值进行遍历
			for (Result result : resultScanner) {
				//给收件箱表的put赋值
				inboxPut.addColumn(Bytes.toBytes(Constants.CONTENT_TABLE_CF), Bytes.toBytes(attend), result.getRow());
			}
		}
		//判断当前的put对象是否为空
		if(!inboxPut.isEmpty()){
			//获取收件箱表
			Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));
			//插入数据
			inboxTable.put(inboxPut);
			//关闭收件箱表
			inboxTable.close();
		}
		//关闭资源
		relaTable.close();
		contTable.close();
		connection.close();
	}	
	
	
	
	//取消关注
	public static void deleteAttends(String uid,String... dels) throws IOException{
		
		//获取Connection对象
		//Connection connection = ConnectionFactory.createConnection(conf);
		
		//用户关系表
		//获取用户关系表对象
		Table relaTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));
		//创建一个集合 存放用户关系表的delete对象
		ArrayList<Delete> relaDelete = new ArrayList<>();
		//创建操作者的delete对象
		Delete uidDelete = new Delete(Bytes.toBytes(uid));
		//循环创建被取关人的delete对象
		for (String del : dels) {
			//给操作者的delete对象赋值
			uidDelete.addColumn(Bytes.toBytes(Constants.RELATION_TABLE_CF1), Bytes.toBytes(del));
			//创建被取关者的delete对象
			Delete delDelete = new Delete(Bytes.toBytes(del));
			//给被取关者的delete对象赋值
			delDelete.addColumn(Bytes.toBytes(Constants.RELATION_TABLE_CF2), Bytes.toBytes(uid));
			//给被取关的delete对象添加到集合
			relaDelete.add(delDelete);
		}
		//将操作者的delete对象添加至集合
		relaDelete.add(uidDelete);
		//执行用户关系表的删除操作
		relaTable.delete(relaDelete);
		
		//收件箱表
		//获取收件箱表
		Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));
		//创建操作者的delete对象
		Delete inboxDelete = new Delete(Bytes.toBytes(uid));
		//给操作者的delete对象赋值
		for (String del : dels) {
			inboxDelete.addColumn(Bytes.toBytes(Constants.INBOX_TABLE_CF), Bytes.toBytes(del));
		}
		//执行收件箱表的删除操作
		inboxTable.delete(inboxDelete);
		//关闭资源
		relaTable.close();
		inboxTable.close();
		connection.close();
		
		
	}
	
	
}
