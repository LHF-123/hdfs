package com.atguigu.hdfsclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

/**
 * @author LHF
 * @date 2020/12/13 17:40
 */
public class HDFSClient {

    private FileSystem fs;

    @Before
    public void before() throws IOException, InterruptedException {
        fs = FileSystem.get(URI.create("hdfs://hadoop102:9000"), new Configuration(), "lhf");
        System.out.println("Before!!!");
    }

    @Test
    public void put() throws IOException, InterruptedException {
        //设置配置文件
        Configuration configuration = new Configuration();
        configuration.setInt("dfs.replication",1);

        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://hadoop102:9000"),
                configuration,"lhf");

        fileSystem.copyFromLocalFile(new Path("d:\\gbk.txt"), new Path("/"));

        fileSystem.close();
    }

    @Test
    public void get() throws IOException, InterruptedException {

        //获取一个HDFS的抽象封装对象
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://hadoop102:9000"),
                configuration, "lhf");

        //用这个对象操作文件系统
        fileSystem.copyToLocalFile(new Path("/test"), new Path("d:\\"));

        //关闭文件系统
        fileSystem.close();
    }

    @Test
    public void rename() throws IOException, InterruptedException {
        //获取文件系统
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://hadoop102:9000"), new Configuration(), "lhf");

        //操作
        fileSystem.rename(new Path("/test"),new Path("/test2"));

        //关闭文件系统
        fileSystem.close();
    }

    @Test
    public void delete() throws IOException {
        boolean delete = fs.delete(new Path("/1.txt"), true);

        if (delete){
            System.out.println("删除成功！");
        } else {
            System.out.println("删除失败！");
        }
    }

    @Test
    public void du() throws IOException {
        FSDataOutputStream append = fs.append(new Path("/test2/1.txt"), 1024);
        FileInputStream open = new FileInputStream("d:\\gbk.txt");
        IOUtils.copyBytes(open,append,1024,true);
    }

    //显示文件和文件夹
    @Test
    public void ls() throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));

        for (FileStatus fileStatus : fileStatuses) {
            if(fileStatus.isFile()){
                System.out.println("以下信息是一个文件信息");
                System.out.println(fileStatus.getPath());
                System.out.println(fileStatus.getLen());
            } else {
                System.out.println("这是一个文件夹");
                System.out.println(fileStatus.getPath());
            }
        }
    }

    //只显示文件
    @Test
    public void listFiles() throws IOException {
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);

        while (files.hasNext()){
            LocatedFileStatus file = files.next();

            System.out.println("=============================");
            System.out.println(file.getPath());

            System.out.println("块消息：");
            BlockLocation[] blockLocations = file.getBlockLocations();

            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                System.out.println("块在：");
                for (String host : hosts) {
                    System.out.println(host + " ");
                }
            }
        }
    }

    @After
    public void after() throws IOException {
        System.out.println("After!!!");
        fs.close();
    }

}
