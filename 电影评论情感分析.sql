#1. 查找同时评论两部电影的用户
SELECT a.uname
,a.message
,b.message
FROM interstellar_travel.interstellar_travel_cleaned a
INNER JOIN the_city_of_love.the_city_of_love_cleaned b ON a.uid = b.uid;

#2. 分析用户对两部电影的情感变化
select a.uname
,a.emotion
,b.emotion
,a.sentiment_score
,b.sentiment_score
,case when a.emotion=b.emotion then '情感一致'
when (a.emotion in ('非常积极','积极') and b.emotion in ('非常消极','消极')) then '积极转消极'
when (a.emotion in ('非常消极','消极') and b.emotion in ('非常积极','积极')) then '消极转积极'
end as cc
from interstellar_travel.interstellar_travel_cleaned a inner join
the_city_of_love.the_city_of_love_cleaned b on a.uid=b.uid
order by  abs(a.sentiment_score-b.sentiment_score) desc;

#3. 比较两部电影的情感分布
select a.emotion
,count(*) as num_comments
,round(avg(a.sentiment_score),2) as a_avg_score
,round(avg(a.like),1) as a_avg_like
from interstellar_travel.interstellar_travel_cleaned a
group by a.emotion
union all
select b.emotion
,count(*) as num_comments
,round(avg(b.sentiment_score),2) as b_avg_score
,round(avg(b.like),1) as b_avg_like
from the_city_of_love.the_city_of_love_cleaned b
group by b.emotion;



#4. 情感与点赞数的关联分析
SELECT
    (SUM((a.sentiment_score - avg_x) * (a.`like` - avg_y)) /
     (SQRT(SUM(POWER(a.sentiment_score - avg_x, 2))) * SQRT(SUM(POWER(a.`like` - avg_y, 2))))) as k_sentiment_a,
    AVG(CASE WHEN a.emotion IN ('非常积极','积极') THEN a.`like` END) as avg_like_pos_a,
    AVG(CASE WHEN a.emotion IN ('非常消极','消极') THEN a.`like` END) as avg_like_neg_a
FROM interstellar_travel.interstellar_travel_cleaned a
CROSS JOIN (
    SELECT
        AVG(sentiment_score) as avg_x,
        AVG(`like`) as avg_y
    FROM interstellar_travel.interstellar_travel_cleaned
) as averages_a
UNION ALL
SELECT
    (SUM((b.sentiment_score - avg_x) * (b.`like` - avg_y)) /
     (SQRT(SUM(POWER(b.sentiment_score - avg_x, 2))) * SQRT(SUM(POWER(b.`like` - avg_y, 2))))) as k_sentiment_b,
    AVG(CASE WHEN b.emotion IN ('非常积极','积极') THEN b.`like` END) as avg_like_pos_b,
    AVG(CASE WHEN b.emotion IN ('非常消极','消极') THEN b.`like` END) as avg_like_neg_b
FROM the_city_of_love.the_city_of_love_cleaned b
CROSS JOIN (
    SELECT
        AVG(sentiment_score) as avg_x,
        AVG(`like`) as avg_y
    FROM the_city_of_love.the_city_of_love_cleaned
) as averages_b;
#皮尔逊洗数均小于0.1，说明二者没有线性相关关系，点赞数与评论的情感倾向（积极或消极）和它获得的点赞数之间没有明显关联。点赞数可能更多地取决于评论本身是否有趣、有洞察力，而不是其情感方向。
#对于电影星际穿越来说，点赞积极评论的请向更高，但无正相关关系，是少量消极评论获得了大量赞影响了平均值分析
#对于爱乐之城，无线性相关关系，评论数点赞差距也不大，双重验证了该电影的评论获赞数与情感无关

