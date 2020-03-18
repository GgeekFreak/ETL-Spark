from pyspark.sql import SparkSession
class PySpark_ETL:
    def __init__(self):
        self.DB_reader(db_name='DTAHUB2018', table_names_list=['FGLEDG'])

        self.Query_Excuter('select EGAIT1 as account_number ,'
                           ' EGACDT as journal_date,'
                           ' EGACAM as Recorded_Amount'
                           ' from FGLEDG '
                           'where EGAIT1 in ("49010020","49010030","40000020")')

        self.Query_Writer(tablename='Infor_GL', mode='overwrite')
    def DB_reader(self, db_name, table_names_list):
        self.spark = SparkSession.builder.appName("Python Spark SQL ETL").config("spark.master", "local[*]"). \
            config("spark.driver.memory", "8g").config("spark.debug.maxToStringFields", "500").getOrCreate()
        self.host = "10.44.62.110"
        self.username = "SRVDTAHUBP"
        self.password = "jdhsjkfh98"
        self.jdbcType = "as400"
        self.driver = "com.ibm.as400.access.AS400JDBCDriver"
        self.connection_type = "jdbc"
        for i, k in enumerate(table_names_list):
            print(i)
            print(k)
            self.spark.read.format("jdbc") \
                .options(url="{}:{}://{}/{};".format(self.connection_type, self.jdbcType, self.host, db_name)
                         , user=self.username, password=self.password,
                         driver=self.driver,
                         dbtable=k).load().createOrReplaceTempView(k)
    def Query_Excuter(self, sql):
        self.newdf = self.spark.sql(sql)
    def Query_Writer(self, tablename, mode):
        load_db_url = "jdbc:mysql://127.0.0.1:3306/ibn_sina"
        load_db_properties = {"user": "root", "password": "root"}
        self.newdf.write.jdbc(url=load_db_url, table=tablename, mode=mode, properties=load_db_properties)

PySpark_ETL()