package com.zzb.hive.scheduling.vo;

import java.io.Serializable;

/**
 * @ClassName Column
 * @Description TODO
 * @Auther zzb
 * @Date 2019/5/13 14:46
 * @Version 1.0
 **/
public class Column implements Serializable {
    //列代码
    private String code;
    //列描述
    private String desc;
    //列id
    private String id;
    //是否主键
    private boolean isPk;
    //列名称
    private String name;
    //列类型
    private String type;

    public Column(){}

    public Column(String code, String desc, String id, boolean isPk, String name, String type) {
        this.code = code;
        this.desc = desc;
        this.id = id;
        this.isPk = isPk;
        this.name = name;
        this.type = type;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
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

    public boolean getIsPk() {
        return isPk;
    }

    public void setIsPk(boolean isPk) {
        isPk = isPk;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return "Column{" +
                "code='" + code + '\'' +
                ", desc='" + desc + '\'' +
                ", id='" + id + '\'' +
                ", isPk=" + isPk +
                ", name='" + name + '\'' +
                ", type='" + type + '\'' +
                '}';
    }
}
