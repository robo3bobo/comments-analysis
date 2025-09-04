import pandas as pd
from snownlp import SnowNLP
import time

# 读取清洗后的CSV文件
df = pd.read_csv(r"C:\Users\samsung\Desktop\实验\星际穿越_cleaned.csv")

print(f"开始情感分析，共 {len(df)} 条评论")


# 更详细的情感分析函数
def detailed_emotion_analysis(text):
    try:
        if pd.isna(text) or str(text).strip() == '':
            return '中性', 0.5

        s = SnowNLP(str(text))
        sentiment_score = s.sentiments

        # 更细致的情感分类
        if sentiment_score > 0.8:
            return '非常积极', sentiment_score
        elif sentiment_score > 0.6:
            return '积极', sentiment_score
        elif sentiment_score > 0.4:
            return '中性', sentiment_score
        elif sentiment_score > 0.2:
            return '消极', sentiment_score
        else:
            return '非常消极', sentiment_score
    except:
        return '中性', 0.5


# 进行分析
emotions = []
scores = []

for i, comment in enumerate(df['message']):
    emotion, score = detailed_emotion_analysis(comment)
    emotions.append(emotion)
    scores.append(score)

    # 显示进度
    if (i + 1) % 100 == 0:
        print(f"已分析 {i + 1}/{len(df)} 条评论")

    # 添加延迟
    if (i + 1) % 50 == 0:
        time.sleep(0.1)

# 添加情感列和情感得分列
df['emotion'] = emotions
df['sentiment_score'] = scores

# 统计情感分布
print("\n情感分析完成！情感分布：")
print(df['emotion'].value_counts())

# 保存结果
output_file = r"C:\Users\samsung\Desktop\实验\星际穿越_cleaned.csv"
df.to_csv(output_file, index=False, encoding='utf-8-sig')
print(f"\n结果已保存到: {output_file}")

# 显示各情感类型的平均得分
print("\n各情感类型平均得分：")
print(df.groupby('emotion')['sentiment_score'].mean())

