import os


class PostgresManager:
    def __init__(self):
        self.jdbc_url = os.getenv('JDBC_URL')
        self.postgresql_url = os.getenv('POSTGRESQL_URL')

    def write_dataframe(self, df, table_name):
        df.write \
            .format("jdbc") \
            .option("url", self.postgresql_url) \
            .option("dbtable", table_name) \
            .option("user", "airflow") \
            .option("password", "airflow") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

    def read_table(self, spark_session, table_name):
        return spark_session.read \
            .format("jdbc") \
            .option("url", self.postgresql_url) \
            .option("dbtable", table_name) \
            .option("user", "airflow") \
            .option("driver", "org.postgresql.Driver") \
            .option("password", "airflow") \
            .load()
