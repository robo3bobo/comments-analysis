import random
import pandas as pd
import numpy as np
import time
import re
import requests
import json
from sqlalchemy import create_engine
import os

# 云端数据库连接配置（请替换为您的实际配置）
cloud_db_connection_string = 'mysql+pymysql://username:password@host:port/database_name'  # 示例

# 发送数据
# 模拟浏览器
headers = {
    "cookie": "enable_web_push=DISABLE; buvid4=EA29ADF6-590B-F5C1-1F1D-3C2B1CF72B8287290-023080621-gJFj3eXopnacVTUKFZN53Q%3D%3D; buvid_fp_plain=undefined; PVID=1; DedeUserID=209313740; DedeUserID__ckMd5=b7281ad49a81438f; fingerprint=b6eda2845bd49c8b986dd4baa51b2fcc; buvid_fp=b6eda2845bd49c8b986dd4baa51b2fcc; CURRENT_QUALITY=80; enable_feed_channel=ENABLE; buvid3=6B7FBF85-E320-6F54-25F4-1634EFADAF9737706infoc; b_nut=1742909137; _uuid=10213292A-109101-102F9-C6BB-D9C73A71CF5B38423infoc; rpdid=|(J|l~|kuJl~0J'u~RlRuY~kY; home_feed_column=4; header_theme_version=OPEN; theme-tip-show=SHOWED; theme-avatar-tip-show=SHOWED; browser_resolution=1389-644; bili_ticket=eyJhbGciOiJIUzI1NiIsImtpZCI6InMwMyIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NTY5NTkzMDgsImlhdCI6MTc1NjcwMDA0OCwicGx0IjotMX0.ZljnFKx1HLe3rrziPZRlsONTZ8a3BjpJ_BnQV7e0iP4; bili_ticket_expires=1756959248; bp_t_offset_209313740=1107507615635603456; SESSDATA=9309b421%2C1772252111%2C4b3f5%2A91CjCT9hkFYQ1jC6cQXPDvuVmWfzN4E9Pdd3mtdqLnQLQmBAkUxNLjB1OsmqBKpYqBCnkSVmtEZVZkNlBoLWtVQ3dsWEtPeW5UcmczWm12MFN3WDZjODdZU3pDZ1hZUTduc3NicFZWeHJPaFNhQzBsSGRZTWVNQART2RklQYkswdlZQSFJtNkFWdWRRIIEC; bili_jct=f5f32fb67441fc8b3f591dd95164ee22; b_lsid=2375F5EC_1990E6AFE2F; sid=8n5mym79; CURRENT_FNVAL=4048",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36 Edg/139.0.0.0"
}

cloud_db_connection_string = "mysql+pymysql://dk:Dingkai456@rm-bp1llqgywhqx6rc9pno.mysql.rds.aliyuncs.com:3306/aiyue_city_comments?charset=utf8mb4"

# 初始化数据库连接
try:
    engine = create_engine(cloud_db_connection_string)
    print("数据库引擎创建成功！")
except Exception as e:
    print('数据库连接失败：', e)
    engine = None

# 创建一个新表用来在云端存数据
table_name = 'aiyue_city_comments'

# 本地CSV文件路径
csv_file_path = 'bilibili_comments.csv'

# 初始URL（使用基础API，避免wbi签名问题）
base_url = 'https://api.bilibili.com/x/v2/reply/main'
next_param = 41  # 初始next值
max_pages = 50  # 总共爬取x页（分x%10次上传）
page_count = 60
all_comments = []  # 存储所有评论数据

while page_count < max_pages:
    # 构造请求参数
    params = {
        'oid': 58467972,
        'type': 1,
        'mode': 3,  # mode=3是按热度排序，通常能获取更多评论
        'next': next_param,
        'plat': 1,
        'web_location': 1315875
    }

    print(f"正在获取第 {page_count + 1} 页数据，next参数: {next_param}")

    # 发送请求
    res = requests.get(url=base_url, headers=headers, params=params)
    # 获取数据
    json_data = res.json()

    # 打印响应状态以便调试
    print(f"响应状态: {json_data.get('code')}, 消息: {json_data.get('message')}")

    # 检查API响应是否成功
    if json_data.get('code') != 0:
        print(f"API请求失败: {json_data.get('message')}")
        break

    # 检查是否存在data字段
    if 'data' not in json_data:
        print("响应中没有data字段")
        break

    # 解析数据
    # 字典取值，提取评论信息所在的replies列表
    replies = json_data['data']['replies']
    print(f"本页获取到 {len(replies)} 条评论")

    for index in replies:
        # 提取具体的评论数据内容
        dit = {
            '昵称': index['member']['uname'],
            '性别': index['member']['sex'],
            '评论内容': index['content']['message'],
            '点赞数': index['like'],
            '用户ID': index['member']['mid'],
            '评论ID': index['rpid'],
            '爬取时间': pd.Timestamp.now()
        }
        if 'reply_control' in index and 'location' in index['reply_control']:
            dit['地区'] = index['reply_control']['location'].replace('IP属地：', '')
        else:
            dit['地区'] = '未知'
        print(dit)
        all_comments.append(dit)

    # 获取下一页的next参数
    if 'cursor' in json_data['data'] and 'next' in json_data['data']['cursor']:
        next_param = json_data['data']['cursor']['next']
        print(f"下一页next参数: {next_param}")
    else:
        print("没有更多评论了")
        break

    page_count += 1

    # 每5页上传一次数据到云端数据库，并保存到本地CSV
    if page_count % 5 == 0 and all_comments:
        try:
            # 将数据转换为DataFrame
            df = pd.DataFrame(all_comments)

            # 上传到云端数据库（使用append模式）
            if engine is not None:
                df.to_sql(name=table_name, con=engine, index=False, if_exists='append')
                print(f'成功上传 {len(all_comments)} 条数据到云端数据库 {table_name}')

            # 保存到本地CSV文件
            # 如果是第一次保存，创建文件并写入表头；否则追加数据
            if not os.path.exists(csv_file_path):
                df.to_csv(csv_file_path, index=False, encoding='utf-8-sig')
                print(f'创建本地CSV文件并保存 {len(all_comments)} 条数据')
            else:
                df.to_csv(csv_file_path, mode='a', header=False, index=False, encoding='utf-8-sig')
                print(f'追加 {len(all_comments)} 条数据到本地CSV文件')

            # 清空缓存，准备下一批数据
            all_comments = []

        except Exception as e:
            print('数据保存失败:', e)

    delay_seconds = random.uniform(2, 5)
    time.sleep(delay_seconds)  # 添加随机延迟避免请求过快

# 爬取结束后，保存剩余的数据（如果有的话）
if all_comments:
    try:
        df = pd.DataFrame(all_comments)

        # 上传剩余数据到云端
        if engine is not None:
            df.to_sql(name=table_name, con=engine, index=False, if_exists='append')
            print(f'上传剩余 {len(all_comments)} 条数据到云端数据库')

        # 保存剩余数据到本地CSV
        if not os.path.exists(csv_file_path):
            df.to_csv(csv_file_path, index=False, encoding='utf-8-sig')
            print(f'创建本地CSV文件并保存剩余 {len(all_comments)} 条数据')
        else:
            df.to_csv(csv_file_path, mode='a', header=False, index=False, encoding='utf-8-sig')
            print(f'追加剩余 {len(all_comments)} 条数据到本地CSV文件')

    except Exception as e:
        print('保存剩余数据失败:', e)

print("爬取完成！数据已保存到云端数据库和本地CSV文件")