package com.zzb.hive.scheduling.plug_in;

import com.zzb.hive.scheduling.action.Hive2DbAction;
import java.io.*;
import java.util.Arrays;
import java.util.HashSet;
import org.apache.hive.com.google.common.collect.Sets;


public class ReadLogFindScripts {
   static String  root = "C:\\Users\\Dell\\Desktop\\backup\\cim_logs";
    public static void main(String[] args) {
       File[] files = new File(root).listFiles();
        if(files.length>0){
            Arrays.asList(files).stream().forEach(f->{
                if(!f.isFile()){
                   Arrays.asList(f.listFiles()).stream().forEach(ff->{
                       //开始处理日志
                       handerLog(ff);
                   });
                }
            });
        }
    }

    /**
     * 处理每一个日期的日志处理完并输出文件
     * @param ff
     */
    private static void handerLog(File ff) {
        //第一步提取全部日志，查出报错误的以及每一个脚本的运行时间
        HashSet<File> resultFiles = Sets.newHashSet();
        for (File file : ff.listFiles()) {
            resultFiles.add(createGlobalLogFileAll(file));
            //createGlobalLogFile
        }
        //第二步写入整体文件
        resultFiles.stream().forEach(file -> {
           // handerResultFile(file);
            writeOneFile(file);
        });
    }

    private static void writeOneFile(File file) {
        BufferedReader br = null;
        String line;
        try {
            br = new BufferedReader(new FileReader(file));
            String name = file.getName();
            File fullFile = new File(file.getParentFile().getAbsoluteFile()+"/" + name.substring(name.lastIndexOf("_") + 1));
            Hive2DbAction.logMsg(fullFile,file.getName()+"=========================="+"\n");
            while(null != (line = br.readLine())){
                    Hive2DbAction.logMsg(fullFile,line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void handerResultFile(File file) {
        BufferedReader br = null;
        String line;
        try {
            br = new BufferedReader(new FileReader(file));
            String name = file.getName();
            File fullFile = new File(file.getParentFile().getAbsoluteFile()+"/" + name.substring(name.lastIndexOf("_") + 1));
            while(null != (line = br.readLine())){
                if(line.indexOf("==")>0){
                    Hive2DbAction.logMsg(fullFile,line);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    //每一日期都要生成一个文件 sql_x_yyyymmdd
    private static File createGlobalLogFileAll(File file) {
        BufferedReader br = null;
        String resultFileName = file.getParentFile().getParentFile().getAbsolutePath()+"_"+ file.getParentFile().getName()+".txt";
        try {
            br = new BufferedReader(new FileReader(file));
            String line;
            Hive2DbAction.logMsg(new File(resultFileName),file.getName());
            while(null != (line = br.readLine())){
                //提取时间
                if(line.contains("Time taken")){
                    if(line.endsWith("seconds")){
                        line = (Double.valueOf(line.substring(line.indexOf(":")+1,line.indexOf("seconds")).trim())>600.0)?line+"==当前脚本大于600s":line;
                    }
                    Hive2DbAction.logMsg(new File(resultFileName),line.trim());
                }
                //提取错误
                if(line.contains("FAILED")||line.contains("CAUSED")||line.contains("failed")){
                    Hive2DbAction.logMsg(new File(resultFileName),line.trim()+"==");
                    break;
                }
            }
            Hive2DbAction.logMsg(new File(resultFileName),"\n\n");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
       return new File(resultFileName);
    }

    /**
     * 每个日期生成两个文件，时间和错误日志分开
     * @param file
     * @return
     */
    private static File createGlobalLogFile(File file) {
        BufferedReader br = null;
        String resultFileName = file.getParentFile().getParentFile().getAbsolutePath()+"_"+ file.getParentFile().getName();
        File timeFile = new File(resultFileName + "_time.txt");
        File errorFile = new File(resultFileName + "_error.txt");
        try {
            br = new BufferedReader(new FileReader(file));
            String line;
            Hive2DbAction.logMsg(timeFile,file.getName());
            Hive2DbAction.logMsg(errorFile,file.getName());
            while(null != (line = br.readLine())){
                //提取时间
                if(line.contains("Time taken")){
                    Hive2DbAction.logMsg(timeFile,line);
                }
                //提取错误
                if(line.contains("FAILED")||line.contains("CAUSED")||line.contains("failed")){
                    Hive2DbAction.logMsg(errorFile,line);
                    break;
                }
            }
            Hive2DbAction.logMsg(timeFile,"\n\n");
            Hive2DbAction.logMsg(errorFile,"\n\n");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new File(resultFileName);
    }
}
