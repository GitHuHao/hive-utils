1.创建 Session级别UDF（每次都要手动创建）
hive (default)> add jar /opt/softwares/hive-1.2.1/lib/hive-utils-1.0-SNAPSHOT.jar;
hive (default)> CREATE TEMPORARY FUNCTION process_names as 'com.bigdata.hive.udtf.NameParserGenericUDTF';

2.rc 环境引入
vim $HIVE_CONF_DIR/.hiverc  (即 hive的conf目录)
------------------------------------------------------------------------------------------
add jar /opt/softwares/hive-1.2.1/lib/hive-utils-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION process_names as 'com.bigdata.hive.udtf.NameParserGenericUDTF';
------------------------------------------------------------------------------------------

3.env环境引入
vim hive-env.sh
------------------------------------------------------------------------------------------
export HIVE_AUX_JARS_PATH=/opt/softwares/hive-1.2.1/lib/hive-utils-1.0-SNAPSHOT.jar
------------------------------------------------------------------------------------------

hive (default)> CREATE TEMPORARY FUNCTION process_names as 'com.bigdata.hive.udtf.NameParserGenericUDTF';

4.下载源码重新编译



