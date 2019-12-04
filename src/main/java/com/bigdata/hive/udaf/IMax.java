package com.bigdata.hive.udaf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDTFOperator;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.FloatWritable;

/**
 *  add jar /opt/softwares/hive-1.2.1/lib/hive-utils-1.0-SNAPSHOT.jar;
 *
 *  create temporary function imax as 'com.bigdata.hive.udaf.IMax';
 *
 *  select imax(id) from (
 *  select 7 id
 *  union all
 *  select 8 id
 *  union all
 *  select 1 id
 *  )a;
 */

public final class IMax extends UDAF {

    public static class MaxiNumberIntUDAFEvaluator implements UDAFEvaluator {

        /**
         * 最终结果
         */
        private FloatWritable result;

        public MaxiNumberIntUDAFEvaluator () {
        }

        /**
         * 负责初始化计算函数并设置它的内部状态，result是存放最终结果的
         */
        public void init () {
            result = null;
        }

        /**
         * 合并两个部分聚集值会调用这个方法
         * @param other
         * @return
         */
        public boolean merge ( FloatWritable other ) {
            return iterate ( other );
        }

        /**
         * 每次对一个新值进行聚集计算都会调用iterate方法
         * @param value
         * @return
         */
        public boolean iterate ( FloatWritable value ) {
            if (value == null)
                return false; // 计算终止信号
            if (result == null)
                result = new FloatWritable ( value.get ( ) ); // 第一次给 result 赋值
            else
                result.set ( Math.max ( result.get ( ) , value.get ( ) ) ); // 行与行对比
            return true;
        }

        /**
         * Hive需要部分聚集结果的时候会调用该方法,会返回一个封装了聚集计算当前状态的对象
         * @return
         */
        public FloatWritable terminatePartial () {
            return result;
        }

        /**
         * Hive需要最终聚集结果时候会调用该方法
         * @return
         */
        public FloatWritable terminate () {
            return result;
        }
    }

}
