package com.zzb.hive.scheduling.plug_in;

import java.io.*;
import java.util.ArrayList;
import org.apache.hive.com.google.common.collect.Lists;

/**
 * 删除未报错脚本，留下报错脚本手动运行
 */
public class DelScript {
    public static void main(String[] args) {
        String path = "C:\\Users\\Dell\\Desktop\\backup\\sql";
        File file = new File(path);
        ArrayList<String> strs = initStrArr(path);
        for (File listFile : file.listFiles()) {
            if(listFile.isDirectory()){
                for (File file1 : listFile.listFiles()) {
                    for (String str : strs) {
                        if(str.equals(file1.getName())){
                            file1.delete();
                        }
                    }
                }
            }
        }
    }

    private static ArrayList<String> initStrArr(String path) {
        File file = new File(path+"\\"+"all.txt");
        ArrayList<String> strs = Lists.newArrayList();
        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line;
            while (null != (line = br.readLine())){
                strs.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return strs;
    }
}
