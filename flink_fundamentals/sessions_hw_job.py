from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, DataTypes, StreamTableEnvironment
from pyflink.table.expressions import col, lit
from pyflink.table.window import Session

def create_processed_events_source_postgres(t_env):
    table_name = 'processed_events'
    source_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            event_timestamp TIMESTAMP(3),
            referrer VARCHAR,
            host VARCHAR,
            url VARCHAR,
            geodata VARCHAR,
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(source_ddl)
    return table_name

def create_sessions_sink_postgres(t_env):
    table_name = 'sessionized_events'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            host VARCHAR,
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            event_count BIGINT,
            session_duration BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name

def create_aggregated_sessions_sink_postgres(t_env):
    table_name = 'aggregated_session_stats'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            host VARCHAR,
            avg_event_count DOUBLE
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name

def sessionize_events():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(1)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        # Create source and sink tables
        source_table = create_processed_events_source_postgres(t_env)
        sessions_sink_table = create_sessions_sink_postgres(t_env)
        aggregated_sessions_sink_table = create_aggregated_sessions_sink_postgres(t_env)

        # Sessionize the events by IP and host with a 5-minute gap
        t_env.from_path(source_table) \
            .window(Session.with_gap(lit(5).minutes).on(col("event_timestamp")).alias("s")) \
            .group_by(col("s"), col("ip"), col("host")) \
            .select(
                col("ip"),
                col("host"),
                col("s").start.alias("session_start"),
                col("s").end.alias("session_end"),
                col("ip").count.alias("event_count"),
                (col("s").end - col("s").start).alias("session_duration")
            ) \
            .execute_insert(sessions_sink_table).wait()

        # Calculate the average number of web events per session and compare between different hosts
        t_env.sql_query(f"""
            SELECT
                host,
                AVG(event_count) AS avg_event_count
            FROM {sessions_sink_table}
            GROUP BY host
        """).execute_insert(aggregated_sessions_sink_table).wait()

    except Exception as e:
        print("Error processing events:", str(e))


if __name__ == '__main__':
    sessionize_events()