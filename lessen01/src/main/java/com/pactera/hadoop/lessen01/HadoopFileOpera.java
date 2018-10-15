package com.pactera.hadoop.lessen01;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HadoopFileOpera {
	
	public static FileSystem link(String url) throws URISyntaxException, IOException {
		URI uri= new URI(url);			//链接 hdfs 的地址
		Configuration conf= new Configuration();	//加载配置文件，不设置则使用默认配置
		
		FileSystem fileSystem = FileSystem.get(uri,conf);	//建立链接动作
		return fileSystem;
	}
	
	
	public static void main(String[] args) {
		try {
			FileSystem fs=HadoopFileOpera.link("hdfs://192.168.43.215:9000");
			HadoopFileOpera.put(fs, "E:/3.txt", "/wordcount_data.txt");
			
			
			
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	//下载文件
	public  static void download(FileSystem fileSystem,String path,String to) throws IOException { 
		FSDataInputStream open = fileSystem.open(new Path(path));	//打开输入路径流
		FileOutputStream out = new FileOutputStream(new File(to));	//打开输出路径流
		IOUtils.copyBytes(open,out, 1024,true);
	}
	
	//上传文件
	public static void put(FileSystem fileSystem,String frompath,String Topath) throws IOException, FileNotFoundException {
		FSDataOutputStream out = fileSystem.create(new Path(Topath));
		InputStream in = new FileInputStream(new File(frompath));
		IOUtils.copyBytes(in , out, 1024 , true);
	}

	//查看文件
	public  static void text(FileSystem fileSystem,String path) throws IOException {
		FSDataInputStream open = fileSystem.open(new Path(path));
		IOUtils.copyBytes(open, System.out, 1024,true);
	}
	
	
	//删除文件	
	public static void rm(FileSystem fileSystem,String path) throws IOException {
		fileSystem.delete(new Path(path), true);
	}
	
	//创建文件夹
	public static void mkdir (FileSystem fileSystem,String path) throws IllegalArgumentException, IOException{
		fileSystem.mkdirs(new Path(path));
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
