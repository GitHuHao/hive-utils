package com.bigdata.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 打 jar 拷贝到 hive lib 目录
 * create temporary function ilower as 'com.bigdata.hive.udf.ILower';
 */

public class ILower extends UDF {

    public String evaluate(final String text) {
        return text.toLowerCase ( );
    }


}
