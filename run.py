#!/usr/bin/python3.6
# -*- coding: utf-8 -*-
# @Time    : 2022/5/11 17:39
# @Author  : Liangliang
# @File    : run.py
# @Software: PyCharm
import argparse
from pyspark.sql import SparkSession
from pytoolkit import TDWSQLProvider
from pytoolkit import TDWUtil
from pytoolkit import TableDesc
from pyspark.sql.types import StructField, StringType, DoubleType, StructType,IntegerType
import pandas
import datetime


def run(args):
    #读取tdw分布式数据库中的表
    print('Begin Load Dataset.')
    spark = SparkSession.builder.appName("read_table").getOrCreate()
    path = args.data_input.split(",")[0]

    db_name = path.split("::")[0]
    td_name = path.split("::")[1]

    data_tdw = TDWSQLProvider(spark, db=db_name, group='tl')

    #得到数据
    data_df = data_tdw.table(td_name).toPandas()

    result = []
    for i in range(1,data_df.shape[0]):
        if data_df.iloc[i,0] == data_df.iloc[i-1,0]:#同一个对象
            if data_df.iloc[i-1,1] <= data_df.iloc[i,1]:#日期不相同
                res_tmp = []
                for j in range(data_df.shape[1]):
                    if j == 0:#id
                        res_tmp.append(data_df.iloc[i,j])
                    elif j == 1:#dteventtime
                        res_tmp.append(data_df.iloc[i,j]+"-"+data_df.iloc[i-1,j])
                    elif j == 12:#chairmanuid
                        res_tmp.append(data_df.iloc[i, j] + "-" + data_df.iloc[i - 1, j])
                    else:
                        res_tmp.append(data_df.iloc[i, j]-data_df.iloc[i - 1, j])
                result.append(res_tmp)
    result = pandas.DataFrame(result) #list->pandas.DataFrame

    fields = [
        StructField("id", StringType(), True),
        StructField("dteventtime", StringType(), True),
        StructField("power", IntegerType(), True),
        StructField("sharecoin", IntegerType(), True),
        StructField("mingcheng_cnt", IntegerType(), True),
        StructField("n", IntegerType(), True),
        StructField("act_n", IntegerType(), True),
        StructField("cnt_r7", IntegerType(), True),
        StructField("connected_n", IntegerType(), True),
        StructField("m", DoubleType(), True),
        StructField("avg_k", DoubleType(), True),
        StructField("cc", DoubleType(), True),
        StructField("chairmanuid", StringType(), True),
        StructField("chairman_active_flag", IntegerType(), True)
    ]
    t_schema = StructType(fields)
    result = spark.createDataFrame(result, t_schema)

    #将result写回tdw表中
    db_names = args.data_output.split("::")[0]
    tb_names = args.data_output.split("::")[1]
    tdwUtil = TDWUtil(user=args.tdw_user, passwd=args.tdw_pwd, dbName=db_names)
    if not tdwUtil.tableExist(tb_names):
        table_desc = TableDesc().setTblName(tb_names). \
            setCols([['id', 'string', 'id'],
                     ['dteventtime', 'string', 'dteventtime'],
                     ['power', 'bigint', 'power'],
                     ['sharecoin', 'bigint', 'sharecoin'],
                     ['mingcheng_cnt', 'bigint', 'mingcheng_cnt'],
                     ['n', 'bigint', 'n'],
                     ['act_n', 'bigint', 'act_n'],
                     ['cnt_r7', 'bigint', 'cnt_r7'],
                     ['connected_n', 'bigint', 'connected_n'],
                     ['m', 'double', 'm'],
                     ['avg_k', 'double', 'avg_k'],
                     ['cc', 'double', 'cc'],
                     ['chairmanuid', 'string', 'chairmanuid'],
                     ['chairman_active_flag', 'bigint', 'chairman_active_flag']]). \
            setComment("This is result!")

        tdwUtil.createTable(table_desc)
        print('Table created')

    print("output db %s table %s" % (db_names, tb_names))
    tdw = TDWSQLProvider(spark, db=db_names, group='tl')
    tdw.saveToTable(result, tb_names)
    print("数据输出完毕!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='算法的参数')
    parser.add_argument("--data_input", help="输入数据的位置", type=str, default='')
    parser.add_argument("--data_output", help="数据的输出位置", type=str, default='')
    parser.add_argument('--tdw_user', type=str, default="", help='tdw_user.')
    parser.add_argument('--tdw_pwd', type=str, default="", help='tdw_pwd.')
    parser.add_argument('--tdw_password', type=str, default="", help='tdw_password.')
    parser.add_argument('--task_name', type=str, default="",
                        help='task_name.')
    parser.add_argument("--tb_log_dir", help="日志位置", type=str, default='')
    args = parser.parse_args()
    start = datetime.datetime.now()
    print("算法的开始时间为:", start)
    run(args)
    end = datetime.datetime.now()
    print("算法的结束时间为:", end)