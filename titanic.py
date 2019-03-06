import pyspark.ml.classification as cl
from pyspark.ml.feature import PCA
from pyspark.ml.feature import StringIndexer,OneHotEncoderEstimator,VectorAssembler
from pyspark.ml import Pipeline

class Titanic:
    def __init__(self,spark,input_data,output_data):
        self.spark = spark
        self.input = input_data
        self.output = output_data

    def load(self):
        self.data_df = self.spark.read.csv(self.input,inferSchema=True,header=True)
        self.data_df.cache()

    def clean(self):
        self.data_df = self.data_df.fillna('S',['Embarked'])
        self.data_df = self.data_df.fillna(29,['Age'])

    def create_preprocessors(self):
        self.stages = []

        cat_cols = ['Sex','Embarked']

        st_list = []
        for col in cat_cols:
            st = StringIndexer(inputCol=col, outputCol=col+'_si')
            st_list.append(st)

        self.stages.extend(st_list)
        
        ohe = OneHotEncoderEstimator(inputCols=['Sex_si','Embarked_si'], \
                outputCols=['Sex_en','Embarked_en'])

        self.stages.append(ohe)

        num_cols = ['Pclass','Age','Fare']

        feature_cols = num_cols + ['Sex_en','Embarked_en']

        va = VectorAssembler(inputCols=feature_cols, outputCol='feature_vec')

        self.stages.append(va)
 
    def dimensionaity_reduction(self):

        pca = PCA(k=3, inputCol='feature_vec', outputCol='feature_data')
        self.stages.append(pca)

    def create_estimators(self):

        logistic = cl.LogisticRegression(maxIter=10, regParam=0.01, labelCol='Survived',featuresCol='feature_data')
        self.stages.append(logistic)
   
    def create_pipeline(self):

        self.pipeline = Pipeline(stages=self.stages)

    def split_data(self):
        return self.data_df.randomSplit([0.7,0.3])

    def fit(self,train):
 
        self.pipeline_model = self.pipeline.fit(train)

    def predict(self,test):

        return self.pipeline_model.transform(test)
