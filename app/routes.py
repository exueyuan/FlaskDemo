# 导入模板模块
from flask import render_template, request
# 从app模块中即从__init__.py中导入创建的app应用
from app import app
from flask import jsonify
from pyspark.sql import SparkSession, functions
# from pyspark.sql.functions import udf
# from pyspark.sql.types import StringType, ArrayType, LongType
# from pyspark.sql.functions import explode
from pyspark.ml import PipelineModel
from pyspark.ml.classification import GBTClassificationModel
from pyspark.sql.types import StringType, ArrayType
from pyspark.sql.functions import udf
import jieba
import pyspark.sql.functions as F
import time
import app.date_utils as date_utils
import random

print("你好")
# 这里写一些宏观定义的参数
spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()

pipeline_path = '/Users/xiaoyin/PycharmProjects/FlaskDemo/pipeline-lisan-date-feature-0830'
loadedPipeline = PipelineModel.load(pipeline_path)

path = "/Users/xiaoyin/PycharmProjects/FlaskDemo/gbt-tree-data-0830"
gbt2 = GBTClassificationModel.load(path)

stop_words = open('/Users/xiaoyin/PycharmProjects/FlaskDemo/stopwords.txt', 'r', encoding='utf_8').readlines()


# 建立路由，通过路由可以执行其覆盖的方法，可以多个路由指向同一个方法
@app.route("/")
@app.route("/index")
def index():
    print("hah")
    user = {'username': 'duke'}
    # 将需要展示的数据传递给模板进行显示
    return render_template('index.html', title='我的', user=user)


@app.route("/get_grade", methods=["GET", "POST"])
def get_grade():
    source_desc = request.args.get('source_desc', "")
    region_name = request.args.get("region_name", "")
    company_name = request.args.get("company_name", "")
    province = request.args.get("province", "")
    city = request.args.get("city", "")
    district = request.args.get("district", "")
    channel = request.args.get("channel", "")
    industry = request.args.get("industry", "")
    is_jd_seller = int(request.args.get("is_jd_seller", "0"))
    clue_type = int(request.args.get("clue_type", "0"))
    retail_dept = int(request.args.get("retail_dept", "0"))
    first_type = request.args.get("first_type", "")
    second_type = request.args.get("second_type", "")
    report_user = request.args.get("report_user", "")
    number = request.args.get("region_name", "")
    create_time = request.args.get("create_time", "")
    hour, month, weekofyear, season, dayofweek = date_utils.formate_time(create_time)

    # 金融
    data = [(source_desc, region_name, company_name,
             province, city, district,
             channel, industry, is_jd_seller,
             clue_type, retail_dept, first_type,
             second_type, report_user, number,
             create_time, hour, month,
             weekofyear, season, dayofweek)]
    print(data)
    # 连接：
    # http://127.0.0.1:5000/get_grade?source_desc=京牛&region_name=浙江省区&company_name=福光塑料&province=浙江&city=金华市&district=义乌市&channel=快递&industry=&is_jd_seller=0&clue_type=0&retail_dept=0&first_type=家居日用/厨具&second_type=&report_user=daichangfei&number=K210628436049&create_time=2021-9-27%2010:30:28
    # columns = ["source_desc", "region_name", "company_name", "province", "city", "district",
    #            "channel", "industry", "is_jd_seller", "clue_type", "retail_dept", "first_type",
    #            "second_type", "report_user", "number", "create_time", "hour", "month",
    #            "weekofyear", "season", "dayofweek", hour, month, weekofyear, season, dayofweek]
    # data = [("京牛", "浙江省区", "福光塑料",
    #          "浙江", "金华市", "义乌市",
    #          "快递", "", 0,
    #          0, 0, "家居日用/厨具",
    #          "", "daichangfei", "K210628436049",
    #          "2021-06-28 17:40:00.0", 17, 6, 26, 2, 1)]
    columns = ["source_desc", "region_name", "company_name",
               "province", "city", "district",
               "channel", "industry", "is_jd_seller",
               "clue_type", "retail_dept", "first_type",
               "second_type", "report_user", "number",
               "create_time", "hour", "month",
               "weekofyear", "season", "dayofweek"]
    df = spark.createDataFrame(data=data, schema=columns)
    df = df.withColumn("new_report_user_rate", functions.lit(0.38))
    df = df.withColumn("new_region_name_rate", functions.lit(0.1))
    df = df.withColumn("new_industry_rate", functions.lit(0.12))
    df = df.withColumn("new_city_rate", functions.lit(0.11))
    df = df.withColumn("new_district_rate", functions.lit(0.28))

    clue_all = df

    def seg(x):
        # s = jieba.lcut(x, cut_all=False, HMM=False)
        # s = [x for x in s if len(x) > 1 and x not in stop_words]
        return []
        # return s

    seg_udf = udf(seg, ArrayType(StringType()))
    clue_all = clue_all.withColumn('first_type', seg_udf(clue_all['first_type']))
    clue_all = clue_all.withColumn('second_type', seg_udf(clue_all['second_type']))
    clue_all = clue_all.withColumn('company_name', seg_udf(clue_all['company_name']))
    clue_all_g = clue_all.groupBy('number').agg(
        F.collect_set('province').alias('set_province'),
        F.collect_set('city').alias('set_city'),
        F.collect_set('district').alias('set_district'),
        F.collect_set('report_user').alias('set_report_user'))
    clue_all_g = clue_all_g.withColumnRenamed('number', 'number_drp')
    df_feat = clue_all.join(clue_all_g, clue_all_g['number_drp'] == clue_all['number'], 'inner')
    df_feat = df_feat.distinct().persist()
    df_join = loadedPipeline.transform(df_feat)
    result = gbt2.transform(df_join)
    df = result.select("prediction")
    prediction = df.head().prediction
    return jsonify({"status": prediction, "code": 200})


# def
@app.route("/get_grade_test", methods=["GET", "POST"])
def grade_gr():
    result = random.randint(0, 1)
    return jsonify({"status": result, "code": 200})
