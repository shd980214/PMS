# -*- coding: utf-8 -*-
"""
Created on Sat May  4 22:40:30 2019

@author: Administrator
"""

import cx_Oracle  # 引用模块cx_Oracle
import pandas as pd
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
def redundancy(column_name_F,column_name_C,F_table,C_table,cur1,conn):
    if column_name_F>=column_name_C:
                cur1.execute("SELECT count(*) from C##SCYW."+F_table)
                data_len1=list(cur1.fetchall()[0])[0]
                cur1.execute("SELECT count(*) from C##SCYW."+C_table)
                data_len2=list(cur1.fetchall()[0])[0]
                if data_len1>=data_len2:
                    inter_column_name=','.join(str(i[0]) for i in list(column_name_C))
                    sql='SELECT '+inter_column_name+' FROM C##SCYW.'+C_table+' INTERSECT SELECT '+inter_column_name+' FROM C##SCYW.'+F_table
                    try:
                        df = pd.read_sql(sql,conn)
                    except:
                          print()
                    else:
                        if len(df)!=0:
                            redundancys={
                                  "F_table":F_table,
                                  "F_data_len":data_len1,
                                  "C_table":C_table,
                                  "C_data_len":data_len2,
                                  "column_lenF":len(column_name_F),
                                  "column_lenC":len(column_name_C),
                                  "percentage_inter_len":len(df)/data_len2,
                                  'inter_len':len(df)
                                         }    
                            redundancy_table_dic.append(redundancys)
def redundancy_table(module_name):
    conn = cx_Oracle.connect('c##LKX/0000@219.216.69.63:1521/orcl')  # 连接数据库
    cur= conn.cursor()
    cur1=conn.cursor()
    table_list=[]
    global redundancy_table_dic
    redundancy_table_dic=[]
    module_pre = module_name
    # 获取当前模块所有表名                                 
    cur.execute(
        "SELECT TABLE_NAME from  all_tab_cols where TABLE_NAME LIKE '" + module_pre + "%' and OWNER='C##SCYW'")  # 用cursor进行各种操作，执行当前括号内的命令
    for result in cur:
        if result[0] not in table_list:
            table_list.append(result[0])
    res=[name for name in table_list if cur.execute("select count(*) from C##SCYW." + name).fetchone()[0]>0]
    print(len(res))
    i=0
    for table_1 in res[:]:   #依次选择一个主表，与剩余表进行比对
        cur.execute("SELECT COLUMN_NAME from  all_tab_cols where TABLE_NAME ='"+table_1+"' and OWNER='C##SCYW'")
        column_name_1=set(cur.fetchall())
        for table_2 in res[i+1:]: #从主表后的剩余表中依次选择一个为对比表
            cur.execute("SELECT COLUMN_NAME from  all_tab_cols where TABLE_NAME ='"+table_2+"' and OWNER='C##SCYW'")
            column_name_2=set(cur.fetchall())
            if len(column_name_1)>len(column_name_2):
                F_table=table_1
                C_table=table_2
                column_name_F=column_name_1
                column_name_C=column_name_2
                redundancy(column_name_F,column_name_C,F_table,C_table,cur1,conn)
            elif len(column_name_1)<len(column_name_2):
                F_table=table_2
                C_table=table_1
                column_name_F=column_name_2
                column_name_C=column_name_1
                redundancy(column_name_F,column_name_C,F_table,C_table,cur1,conn)
            elif column_name_1==column_name_2:
                cur1.execute("SELECT count(*) from C##SCYW."+table_1)
                data_len1=list(cur1.fetchall()[0])[0]
                cur1.execute("SELECT count(*) from C##SCYW."+table_2)
                data_len2=list(cur1.fetchall()[0])[0]
                if data_len1>=data_len2:
                    F_table=table_1
                    C_table=table_2
                else:
                    F_table=table_2
                    C_table=table_1
                sql="SELECT * FROM "+"C##SCYW."+F_table+" INTERSECT "+"SELECT * FROM "+"C##SCYW."+C_table
                df=pd.read_sql(sql,conn)
                if len(df)!=0:
                    redundancys={
                                  "F_table":F_table,
                                  "F_data_len":max(data_len1,data_len2),
                                  "C_table":C_table,
                                  "C_data_len":min(data_len1,data_len2),
                                  "column_len":len(column_name_1),
                                  "percentage_inter_len":len(df)/min(data_len1,data_len2),
                                  "inter_len":len(df)
                                         }    
                    redundancy_table_dic.append(redundancys)  
        i+=1
    pd1=pd.DataFrame(redundancy_table_dic)
    print(pd1)
    #if not pd1.empty:
    pd1.to_excel("redundancy_table_"+module_pre+".xls")
    return pd1
    cur.close()
    conn.close()
#model_list=['T_ZH','T_PWGM','T_PWYW','T_PWBZH','T_MXPWYW','T_EDC','MV_ZH','T_SB','T_MXPMS','T_XB','OPC_MODEL','ISC_SPECIALORG','T_ZB','T_YJ','T_ZHJH','T_TD','T_PWJX','T_PROC','ISC_PMS','T_PWYX','T_DW','T_PWGC','T_SYHZB','T_EFILE','T_DTOOLS','ISC_USER',]
#for i in model_list:
#   redundancy_table(i) 
redundancy_table('T_CMS')   

            
