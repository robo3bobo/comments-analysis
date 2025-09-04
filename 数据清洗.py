import pandas as pd
import re

# 定义要清洗的文件列表
file_list = [r"C:\Users\samsung\Desktop\实验\星际穿越脏.csv", r"C:\Users\samsung\Desktop\实验\爱乐之城脏.csv"]  # 替换为你的实际文件名

for file_name in file_list:
    print(f"正在清洗文件: {file_name}")

    try:
        # 读取CSV文件
        df = pd.read_csv(file_name)
        print(f"原始数据形状: {df.shape}")

        # 1. 去除重复评论
        df = df.drop_duplicates(subset=['message', 'uid'])
        print(f"去重后数据形状: {df.shape}")


        # 3. 去除评论内容中的特殊符号
        df['message'] = df['message'].apply(
            lambda x: re.sub(r'[^\u4e00-\u9fa5a-zA-Z0-9\s，。！？；："''"（）《》]', '', str(x)).strip()
            if pd.notna(x) else x
        )

        # 4. 去除空评论
        df = df[df['message'].str.strip().astype(bool)]

        # 5. 重置索引
        df = df.reset_index(drop=True)

        # 生成清洗后的文件名
        cleaned_file_name = file_name.replace('.csv', '_cleaned.csv')

        # 保存清洗后的数据
        df.to_csv(cleaned_file_name, index=False, encoding='utf-8-sig')
        print(f"清洗完成！已保存为: {cleaned_file_name}")
        print(f"最终数据形状: {df.shape}")
        print("-" * 50)

    except FileNotFoundError:
        print(f"文件 {file_name} 不存在，跳过...")
        print("-" * 50)
    except Exception as e:
        print(f"处理文件 {file_name} 时出错: {e}")
        print("-" * 50)

print("所有文件清洗完成！")