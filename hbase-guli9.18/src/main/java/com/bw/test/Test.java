package com.bw.test;

import java.io.IOException;

import com.bw.constants.Constants;
import com.bw.dao.Dao;
import com.bw.utils.HBaseUtil;

public class Test {
	
	public static void main(String[] args) throws IOException {
		
		//建表
		//init();
		//发布微博
		publishWeiBo();
		//关注
		//addAttends();
		deleteAttendsTest();
	}
	
	//建表
	public static void init() throws IOException{
		//创建命名空间
		//HBaseUtil.createNameSpace(Constants.NAMESPACE);
		
		
		//创建微博内容表
		HBaseUtil.createTable(Constants.CONTENT_TABLE, 
				Constants.CONTENT_TABLE_VERSION, Constants.CONTENT_TABLE_CF);
		System.out.println(HBaseUtil.isTableExist(Constants.CONTENT_TABLE));
		//创建用户关系表
		HBaseUtil.createTable(Constants.RELATION_TABLE, 
				Constants.RELATION_TABLE_VERSION, Constants.RELATION_TABLE_CF1,Constants.RELATION_TABLE_CF2);
		System.out.println(HBaseUtil.isTableExist(Constants.RELATION_TABLE));
		//创建收件箱表
		HBaseUtil.createTable(Constants.INBOX_TABLE, 
				Constants.CONTENT_TABLE_VERSION, Constants.CONTENT_TABLE_CF);
		System.out.println(HBaseUtil.isTableExist(Constants.INBOX_TABLE));
	}
	
	//发布微博
	public static void publishWeiBo() throws IOException{
		Dao.publishWeiBo("1003", "I don't want to go to class.");
		System.out.println("发布成功...");
	}
	
	//关注
	public static void addAttends() throws IOException{
		Dao.addAttends("1002", "1001","1003");
		System.out.println("关注成功");
	}
	
	//
	public static void deleteAttendsTest() throws IOException{
		Dao.deleteAttends("1002", "1001");
		
	}

}
