package com.zzb.hive.scheduling.vo;
import java.io.Serializable;
import java.util.List;

/**
 * @ClassName Table
 * @Description TODO
 * @Auther zzb
 * @Date 2019/5/13 14:46
 * @Version 1.0
 **/
public class Table implements Serializable {
    //表代码
    private String code;
    //所有列
    private List<Column> columns;
    //描述
    private String desc;
    //id
    private String id;
    //表名
    private String name;

    @Override
    public String toString() {
        return "Table{" +
                "code='" + code + '\'' +
                ", columns=" + columns +
                ", desc='" + desc + '\'' +
                ", id='" + id + '\'' +
                ", name='" + name + '\'' +
                '}';
    }

    public Table() {
    }

    public Table(String code, List<Column> columns, String desc, String id, String name) {
        this.code = code;
        this.columns = columns;
        this.desc = desc;
        this.id = id;
        this.name = name;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
