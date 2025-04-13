from psycopg2 import sql
import psycopg2
import csv
import os
import io

class PostgresManager:
    def __init__(self):
        self.jdbc_url = os.getenv('JDBC_URL')
        self.postgresql_url = os.getenv('POSTGRESQL_URL')
        self._conn_params = self._parse_postgres_url()
        self._schema_cache = {}  # Cache for table schemas

    def _parse_postgres_url(self):
        """Extract connection parameters from PostgreSQL URL"""
        parts = self.postgresql_url.split('//')[1].split('/')[0].split(':')
        host = parts[0]
        port = parts[1] if len(parts) > 1 else '5432'
        dbname = self.postgresql_url.split('/')[-1]
        return {
            'host': host,
            'port': port,
            'dbname': dbname,
            'user': 'airflow',
            'password': 'airflow'
        }

    def _get_pg_connection(self):
        """Get raw PostgreSQL connection"""
        return psycopg2.connect(**self._conn_params)

    def _table_exists(self, table_name):
        """Check if table exists in PostgreSQL"""
        with self._get_pg_connection() as conn, conn.cursor() as cursor:
            cursor.execute("""
                SELECT EXISTS (
                    SELECT 1 
                    FROM information_schema.tables 
                    WHERE table_name = %s
                );
            """, (table_name.lower(),))
            return cursor.fetchone()[0]

    def _create_table(self, df, table_name):
        """Create table with proper schema based on DataFrame"""
        if table_name in self._schema_cache:
            return  # Already created in this session

        with self._get_pg_connection() as conn, conn.cursor() as cursor:
            # Generate PostgreSQL DDL from Spark schema
            fields = []
            for field in df.schema:
                pg_type = self._spark_to_postgres_type(field.dataType)
                fields.append(sql.Identifier(field.name).as_string(conn) + " " + pg_type)
            
            create_stmt = sql.SQL("""
                CREATE TABLE IF NOT EXISTS {table} (
                    {fields}
                )
            """).format(
                table=sql.Identifier(table_name),
                fields=sql.SQL(',\n').join([sql.SQL(f) for f in fields])
            )

            cursor.execute(create_stmt)
            conn.commit()
            self._schema_cache[table_name] = True

    def _spark_to_postgres_type(self, spark_type):
        """Convert Spark DataType to PostgreSQL type using correct type names"""
        type_map = {
            'string': 'TEXT',
            'integer': 'INTEGER',
            'long': 'BIGINT',
            'double': 'DOUBLE PRECISION',
            'float': 'REAL',
            'boolean': 'BOOLEAN',
            'timestamp': 'TIMESTAMP',
            'date': 'DATE',
            'decimal': 'NUMERIC',
            'short': 'SMALLINT',
            'byte': 'SMALLINT'
        }
        return type_map.get(spark_type.typeName().lower(), 'TEXT')

    def write_dataframe(self, df, table_name):
        """Optimized write with automatic table creation"""
        table_name = table_name.lower()
        
        # Check cache first to avoid repeated DB checks
        if not self._schema_cache.get(table_name):
            if not self._table_exists(table_name):
                self._create_table(df, table_name)

        # Batch write configuration
        df.write \
            .format("jdbc") \
            .option("url", self.postgresql_url) \
            .option("dbtable", table_name) \
            .option("user", "airflow") \
            .option("password", "airflow") \
            .option("driver", "org.postgresql.Driver") \
            .option("batchsize", 10000) \
            .option("rewriteBatchedInserts", "true") \
            .option("quoteIdentifiers", "false") \
            .mode("append") \
            .save()

        # Update cache
        self._schema_cache[table_name] = True

    def read_table(self, spark_session, table_name):
        """Optimized read with predicate pushdown"""
        table_name = table_name.lower()
        return spark_session.read \
            .format("jdbc") \
            .option("url", self.postgresql_url) \
            .option("dbtable", table_name) \
            .option("user", "airflow") \
            .option("password", "airflow") \
            .option("driver", "org.postgresql.Driver") \
            .option("quoteIdentifiers", "false") \
            .option("fetchsize", 10000) \
            .load()

    def optimize_table(self, table_name):
        """Perform PostgreSQL table optimization"""
        table_name = table_name.lower()
        with self._get_pg_connection() as conn, conn.cursor() as cursor:
            cursor.execute(sql.SQL("""
                VACUUM ANALYZE {table};
                REINDEX TABLE {table};
            """).format(table=sql.Identifier(table_name)))
            conn.commit()