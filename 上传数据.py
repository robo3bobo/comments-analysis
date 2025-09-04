#数据入库
from logging import exception

import pandas as pd
from sqlalchemy import create_engine, false

#读取本地文件
file_path=r"C:\Users\samsung\Desktop\实验\the_city_of_love_cleaned.csv"
df=pd.read_csv(file_path)

#快速查看数据，确认读取成功
print("EV数据预览：")
print(df.head(5))
print("\nEV数据框结构：")
print(df.info())

#数据清洗，处理缺失值
print('\n缺失值统计：')
print(df.isnull().sum())
print(f"\n重复行数量：{df.duplicated().sum()}")
#数据无缺失值，无重复行，是干净清楚的数据源

# 4. 创建连接云端数据库的引擎

cloud_db_connection_string = "mysql+pymysql://dk:Dingkai456@rm-bp1llqgywhqx6rc9pno.mysql.rds.aliyuncs.com:3306/the_love_of_city"
#这里要记得在云端建立一个名称相同的新的数据库

try:
    engine=create_engine(cloud_db_connection_string)
    print("数据库引擎创建成功！")
except Exception as e:
    print('数据库连接失败：',e)
#已经可以正常连接至云端数据库


#创建一个新表用来在云端存数据
table_name='the_city_of_love_cleaned'
try:
    df.to_sql(name=table_name,con=engine,index=False,if_exists='replace')
    print('数据成功写入云端')
except Exception as e:
    print('写入失败',e)

