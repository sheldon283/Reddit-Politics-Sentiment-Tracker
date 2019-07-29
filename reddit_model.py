from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf, substring, col
from pyspark.sql.types import ArrayType, StringType, IntegerType, FloatType, BooleanType
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder, CrossValidatorModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator


# TASK 1
# Code for task 1…
def task1(sqlContext):
    try:
        comments = sqlContext.read.parquet("comments")
    except:
        comments = sqlContext.read.json("comments-minimal.json.bz2")
        comments.write.parquet("comments")
    try:
        submissions = sqlContext.read.parquet("submissions")
    except:
        submissions = sqlContext.read.json("submissions.json.bz2")
        submissions.write.parquet("submissions")
    
    try:
        labeled_data = sqlContext.read.parquet("data")
    except:
        labeled_data = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('labeled_data.csv')
        labeled_data.write.parquet("data")
    return comments, labeled_data, submissions

# TASK 2
# Code for task 2…
def task2(sqlContext, comments, labeled_data):
    data = (comments.join(labeled_data, comments["id"] == labeled_data["Input_id"])).select("Input_id", "labeldem", "labelgop", "labeldjt", "body", "author_flair_text")
    return data

# TASK 4, 5
# Code for task 4 and 5…
def task45(sqlContext, data):
    from cleantext import sanitize
    sanitizeudf = udf(sanitize, ArrayType(StringType()))
    #https://stackoverflow.com/questions/53422473/pyspark-dataframe-create-new-column-based-on-function-return-value
    
    data = data.withColumn("grams", sanitizeudf(data.body))
    return data

# TASK 6A
# Code for task 6A…
def task6a(sqlContext, data):
    cv = CountVectorizer(inputCol="grams", outputCol="count_vectors", minDF=10, binary=True)
    model = cv.fit(data)
    result = model.transform(data)
    result.show(n=10)
    return result, model

def convertpos(djt):
    if djt == 1:
        return 1
    else:
        return 0

def convertneg(djt):
    if djt == -1:
        return 1
    else:
        return 0

# TASK 6B
# Code for task 6B…
def task6B(sqlContext, data):
    udf_func_pos = udf(convertpos, IntegerType())
    udf_func_neg = udf(convertneg, IntegerType())
    data = data.withColumn("positive_label", udf_func_pos(data.labeldjt))
    data = data.withColumn("negative_label", udf_func_neg(data.labeldjt))
    return data

# TASK 7
# Code for task 7…
def task7(sqlContext, data):

    # Initialize two logistic regression models.
    # Replace labelCol with the column containing the label, and featuresCol with the column containing the features.
    poslr = LogisticRegression(labelCol="positive_label", featuresCol="count_vectors", maxIter=10)
    neglr = LogisticRegression(labelCol="negative_label", featuresCol="count_vectors", maxIter=10)
    # This is a binary classifier so we need an evaluator that knows how to deal with binary classifiers.
    posEvaluator = BinaryClassificationEvaluator(labelCol="positive_label")
    negEvaluator = BinaryClassificationEvaluator(labelCol="negative_label")
    # There are a few parameters associated with logistic regression. We do not know what they are a priori.
    # We do a grid search to find the best parameters. We can replace [1.0] with a list of values to try.
    # We will assume the parameter is 1.0. Grid search takes forever.
    posParamGrid = ParamGridBuilder().addGrid(poslr.regParam, [1.0]).build()
    negParamGrid = ParamGridBuilder().addGrid(neglr.regParam, [1.0]).build()
    # We initialize a 5 fold cross-validation pipeline.
    posCrossval = CrossValidator(
                             estimator=poslr,
                             evaluator=posEvaluator,
                             estimatorParamMaps=posParamGrid,
                             numFolds=5)
    negCrossval = CrossValidator(
                             estimator=neglr,
                             evaluator=negEvaluator,
                             estimatorParamMaps=negParamGrid,
                             numFolds=5)
    # Although crossvalidation creates its own train/test sets for
    # tuning, we still need a labeled test set, because it is not
    # accessible from the crossvalidator (argh!)
    # Split the data 50/50
    posTrain, posTest = data.randomSplit([0.5, 0.5])
    negTrain, negTest = data.randomSplit([0.5, 0.5])
    # Train the models
    print("Training positive classifier...")
    posModel = posCrossval.fit(posTrain)
    print("Training negative classifier...")
    negModel = negCrossval.fit(negTrain)

    # Once we train the models, we don't want to do it again. We can save the models and load them again later.
    posModel.save("project2/pos.model")
    negModel.save("project2/neg.model")

# TASK 8
# Code for task 8…
def task8(sqlContext, comments, submissions):
    #comments.createOrReplaceTempView("Comments")
    #comments = comments.sample(False, 0.002, None)
    udf_n = udf(remove_three, StringType())
    comments_with_new = comments.withColumn("link_id_new", udf_n(comments.link_id))
    comments_with_new.createOrReplaceTempView("Comments")
    submissions.createOrReplaceTempView("Submissions")
    entire_file = sqlContext.sql("SELECT COALESCE(Comments.author_flair_text,Submissions.author_flair_text, null) as author_flair_text, Comments.score as commentsScore, submissions.score as submissionsScore, Comments.created_utc, Submissions.title, Submissions.id, Comments.body FROM Comments JOIN Submissions ON Comments.link_id_new = Submissions.id")
    return entire_file

def remove_three(str):
    if len(str) > 3:
        return str[3:]
    else:
        return str

def getPosProbs(prob):
    if (prob > 0.20):
        return 1
    return 0

def getNegProbs(prob):
    if (prob > 0.25):
        return 1
    return 0

# TASK 9
# Code for task 9…
def task9(context, entire_file, model):
    posModel = CrossValidatorModel.load("project2/pos.model")
    negModel = CrossValidatorModel.load("project2/neg.model")
    data = task45(context, entire_file)
    rr_data = model.transform(data)
    rr_data.createOrReplaceTempView("Data")
    new_data = context.sql("SELECT * FROM Data WHERE Data.body NOT LIKE '%&gt%' OR Data.body NOT LIKE '%/s%'")
    posResult = posModel.transform(new_data)
    posResult.createOrReplaceTempView("Pos")
    new_data_with_pos = context.sql("SELECT author_flair_text, created_utc, submissionsScore, commentsScore, title, id, body, grams, count_vectors, probability as prob_pos FROM Pos")
    result = negModel.transform(new_data_with_pos)
    result.createOrReplaceTempView("Result")
    formatted_result = context.sql("SELECT author_flair_text, created_utc, title, submissionsScore, commentsScore, id, body, grams, count_vectors, prob_pos, probability as prob_neg FROM Result")
    
    # Used this link to get the first element in the probability column
    # https://stackoverflow.com/questions/44425159/access-element-of-a-vector-in-a-spark-dataframe-logistic-regression-probability?noredirect=1&lq=1
    firstelement=udf(lambda v:float(v[1]),FloatType())
    
    pos_udf = udf(getPosProbs, IntegerType())
    neg_udf = udf(getNegProbs, IntegerType())
    
    res = formatted_result.select(firstelement('prob_neg'), firstelement('prob_pos'), 'author_flair_text', 'created_utc', 'title', 'id', 'body', 'count_vectors', 'commentsScore', 'submissionsScore')
    nResult = res.withColumn("pos", pos_udf(res["<lambda>(prob_pos)"]))
    new_result = nResult.withColumn("neg", neg_udf(nResult["<lambda>(prob_neg)"]))
    
    new_result.createOrReplaceTempView("NewResult")
    
    actual_result = context.sql("SELECT author_flair_text, created_utc, title, id, body, count_vectors, commentsScore, submissionsScore, pos, neg FROM NewResult" )
    
    #new_data.show(n=10)
    return actual_result

# TASK 10
# Code for task 10…
def task10(context, results):
    results.createOrReplaceTempView("actResults")
    ## percentage by id
    first_data = context.sql("SELECT id, 100*AVG(pos) as poscount, 100*AVG(neg) as negcount FROM actResults GROUP BY id")
    first_data.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("average.csv")
    
    ## percentage by date
    second_data = context.sql("SELECT FROM_UNIXTIME(created_utc, 'yyyy-MM-dd') as date, 100*AVG(pos) as poscount, 100*AVG(neg) as negcount FROM actResults GROUP BY date")
    
    second_data.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("time_data.csv")
    
    ## percentage by state
    context.registerFunction("isState", (lambda x: x in ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 'District of Columbia', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming']) , BooleanType())
    
    third_data = context.sql("SELECT author_flair_text as state, 100*AVG(pos) as poscount, 100*AVG(neg) as negcount FROM actResults WHERE (isState(author_flair_text)) GROUP BY author_flair_text")
    
    third_data.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("state_data.csv")

    #percentage by comment score
    comm_score = context.sql("SELECT commentsScore, 100*AVG(pos) as poscount, 100*AVG(neg) as negcount FROM actResults GROUP BY commentsScore")
    
    comm_score.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("comment_score.csv")
    
    #percentage by submission score
    sub_score = context.sql("SELECT submissionsScore, 100*AVG(pos) as poscount, 100*AVG(neg) as negcount FROM actResults GROUP BY submissionsScore")

    sub_score.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("submission_score.csv")

# Gets Top 10 stories for positive and negative for the report
def getTop10Stories(context, results):
    results.createOrReplaceTempView("actResults")
    pro = context.sql("SELECT title, 100*AVG(pos) as poscount, 100*AVG(neg) as negcount FROM actResults GROUP BY title ORDER BY 100*AVG(pos) DESC")
    
    pro.show(n=10, truncate=False)

    neg = pro.orderBy("negcount", ascending=False)

    neg.show(n=10, truncate=False)


def main(context):
    """Main function takes a Spark SQL context."""
    comments, labeled_data, submissions = task1(context)
    data = task2(context, comments, labeled_data)
    data_with_grams = task45(context, data)
    data_with_vectorizers, count_vect_model = task6a(context, data_with_grams)
    data_with_pos_neg = task6B(context, data_with_vectorizers)
    task7(context, data_with_pos_neg)
    entire_file = task8(context, comments, submissions)
    total_results = task9(context, entire_file, count_vect_model)
    task10(context, total_results)
    getTop10Stories(context, total_results)

# YOUR CODE HERE
# YOU MAY ADD OTHER FUNCTIONS AS NEEDED

if __name__ == "__main__":
    conf = SparkConf().setAppName("CS143 Project 2B")
    conf = conf.setMaster("local[*]")
    sc   = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    sc.addPyFile("cleantext.py")
    main(sqlContext)







