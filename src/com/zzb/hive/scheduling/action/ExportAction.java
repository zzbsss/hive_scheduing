package com.zzb.hive.scheduling.action;

import com.zzb.hive.scheduling.service.TableToExcelService;
import com.zzb.hive.scheduling.service.impl.TableToExcelImpl;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

/**
 * <p>Title：ExportAction</p>
 * <p>Description：将表结构信息导入到Excel</p>
 * <p>Company：com.yxcy.importdata.action</p>
 *
 * @author
 * @version 1.0
 * <p>Improvement：</p>
 * @date 2019/5/19 16:06
 */
public class ExportAction {

    /*创建Excel对象*/
    private  Workbook book = new HSSFWorkbook();

    /*创建流对象*/
    private  FileOutputStream fileOutputStream;


    /**
     * <p>说明:   获取Excel文件</p><br/>
     * <p>参数:   [dbType]数据库类型枚举</p><br/>
     * <p>返回值: java.io.FileOutputStream返回Excel文件流对象</p><br/>
     * <p>改进:   </p>
     */
    private  FileOutputStream getFileOutputStream(String  dbType) throws FileNotFoundException {
        switch (dbType) {
            case "hive":
                fileOutputStream = new FileOutputStream("/home/mdrh/mdrh/mdrh/excel/hive.xls");
                break;
            case "oracle":
                fileOutputStream = new FileOutputStream("/home/mdrh/mdrh/mdrh/excel/oracle.xls");
                break;
            case "mysql":
                fileOutputStream = new FileOutputStream("/home/mdrh/mdrh/mdrh/excel/mysql.xls");
                break;
        }
        return fileOutputStream;
    }


    /**
     * 说明:
     * 参数:       [tbName, args] tbName表名(必需)， args可选参数[数据库类型(必需的)][数据库名称(若类型为hive则是必需的)]
     * 返回值:      void
     * 改进:
     */
    public  void exportToExcel(String tbName, String... args) {
        FileOutputStream fileOutputStream = null;
        try {
             fileOutputStream = getFileOutputStream(args[0]);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        TableToExcelService tableToExcelService = new TableToExcelImpl();
        List<String[]> list = tableToExcelService.queryTableStructure(tbName, args);
        String[] cells = {"列名", "数据类型", "是否可为空", "列说明", "约束名"};
        /*创建excel*/
        book = new HSSFWorkbook();
        /*创建分表*/
        Sheet sheet = book.createSheet(tbName);
        /*轮询设置表头*/
        Row row = sheet.createRow(0);
        for (int i = 0; i < cells.length; i++) {
            row.createCell(i).setCellValue(cells[i]);
        }
        /*创建行、行的单元格、并设置单元格的值*/
        for (int i = 0; i < list.size(); i++) {
            String[] str = list.get(i);
            row = sheet.createRow(i + 1);
            for (int j = 0; j < str.length; j++) {
                row.createCell(j).setCellValue(str[j]);
            }
        }
        try {
            book.write(fileOutputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(tbName + "表结构信息导入Excel完成！");
    }
}
