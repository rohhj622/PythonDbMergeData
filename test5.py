import cx_Oracle
import os
import pandas as pd
from datetime import datetime
import math
import time

def __create_engine_ora(self):
    cx_Oracle.init_oracle_client(lib_dir=r"D:\instantclient_12_2")
    oracle_connection_string = (
            'oracle+cx_oracle://username:password@' +
            cx_Oracle.makedsn(IP, PORT, service_name='orcl')
    )

    engine = create_engine(
            oracle_connection_string.format(
            username=username,
            password=password,
            hostname=IP,
            port=PORT,
            service_name='orcl',
        ), use_batch_mode=True
    )

    return engine

# 64bit Oracle Clinet 미설치시 사용
LOCATION = r"D:\instantclient_12_2"
os.environ["PATH"] = LOCATION + ";" + os.environ["PATH"]

#os.environ["NLS_LANG"]="AMERICAN_AMERICA.AL32UTF8"

def insertData(d):
    try:
        conn = cx_Oracle.connect("username/password@IP:PORT/orcl")
        cursor=conn.cursor()

        #sql_1 = "INSERT INTO INFOAPP." + tableNm + " " + colNm+ " VALUES (:1, :2 ,:3, :4)"
        """sql_1 = "MERGE INTO "+tableNm+" S USING DUAL "
        sql_1 = sql_1 + "ON (S.F_DAM = :2 AND S.F_ITEM = :3 AND S.F_DATETIME = :1) "
        sql_1 = sql_1 + "WHEN MATCHED THEN "
        sql_1 = sql_1 + "UPDATE SET S.F_VALUE = :4 " 
        sql_1 = sql_1 + "WHEN NOT MATCHED THEN "
        sql_1 = sql_1 + "INSERT (S.F_DAM, S.F_ITEM, S.F_DATETIME, S.F_VALUE) "
        sql_1 = sql_1 + "VALUES (:2, :3, :1, :4) " """

            
        sql_1 = "MERGE INTO "+tableNm+" S USING DUAL "
        sql_1 = sql_1 + "ON (S.F_DAM = :1 AND S.F_ITEM = :2 AND S.F_DATETIME = :3) "
        sql_1 = sql_1 + "WHEN MATCHED THEN "
        sql_1 = sql_1 + "UPDATE SET S.F_VALUE = :4 " 
        sql_1 = sql_1 + "WHEN NOT MATCHED THEN "
        sql_1 = sql_1 + "INSERT (S.F_DAM, S.F_ITEM, S.F_DATETIME, S.F_VALUE) "
        sql_1 = sql_1 + "VALUES (:5, :6, :7, :8) "
        
        # print(sql_1)
        #print("'"+folderNm+"/"+i+"' INSERT START")
        print("-- INSERT START")
        start = time.time()
        
        cursor.executemany(sql_1, d)
        
        conn.commit()
        
        end = time.time()
        print(f"-- INSERT FINISH (DURING {end - start:.5f} SEC)")
        print("=======================================")
    except Exception as e:
        print(e)
        print("-- ERROR")
        os.system("pause")
        #print("'"+folderNm+"/"+i+"' ERROR!!!!!!!!!")
    finally:
        cursor.close()
        conn.close()
        #print(f"-- INSERT FINISH (DURING {end - start:.5f} SEC)")
        #print("'"+folderNm+"/"+i+"' FINISH")

    
folderNm = './INSERT_DATA'
fileList = os.listdir(folderNm)
tableNm = ""
colNm = ""

print("FILE LIST")
print(fileList)
print("=======================================")
for i in fileList:
    dataSet = []
    with open(folderNm+"/"+i, mode='r', encoding='utf8') as file1:
        print("FILE  NAME : " + i)
        lines = file1.readlines()

        for k in range(2,len(lines)):
            line = lines[k].split("(")
            a = lines[k].split("'")[1]
            a = datetime.strptime(a, '%Y-%m-%d %H:%M:%S')

            b = lines[k].split("'")[5]
            c = lines[k].split("'")[7]
            d = lines[k].split("'")[8].split(",")[1].split(")")[0]

            d1 = 1;
            
            if (d == 'null') or (d == ''):
                d1 = None
            else:
                d1 = float(d)
            
            #strList = [a,b,c,d1] # 시간, 댐, item, value
            strList = [b, c, a, d1, b, c, a, d1] # 시간, 댐, item, value
            #print(strList)
            if k == 2 :
                tableNm = lines[k].split(" ")[2] # T_IROS_RAW_DAY
                tableNm = i.split(".")[0]
                colNm = "("+line[1].split(")")[0]+")"
                print("TABLE NAME : " + tableNm)
                # print("=======================================")
                
            dataSet.append(strList)
        insertData(dataSet)

        #os.system("pause")
os.system("pause")