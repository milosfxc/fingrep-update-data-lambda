import psycopg2
from db_ops import get_db_connection
from typing import List, Dict, Any
import uuid
from io import StringIO
import csv
from psycopg2.extras import execute_values
from config import logger

def upsert_data_smart(data: List[Dict[str, Any]], table_name: str, conflict_columns: set,
                      threshold: int = 1000) -> bool:
    """
    Smart upsert that automatically chooses the best method based on data size
    """
    if not data:
        logger.warning('upsert_data_smart - No data to upsert')
        return False

    # Use optimized method for large datasets
    if len(data) > threshold:
        return _upsert_large_dataset(data, table_name, conflict_columns)
    else:
        return _upsert_small_dataset(data, table_name, conflict_columns)


def _upsert_small_dataset(data: List[Dict[str, Any]], table_name: str, conflict_columns: set) -> bool:
    """For small datasets - uses execute_batch"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                columns = list(data[0].keys())
                missing_conflict_columns = conflict_columns - set(columns)
                if missing_conflict_columns:
                    raise ValueError(f"Conflict columns not found in data: {missing_conflict_columns}")

                # Build query
                conflict_columns_str = ', '.join(conflict_columns)
                update_columns = [col for col in columns if col not in conflict_columns]
                update_set = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])

                insert_query = f"""
                    INSERT INTO {table_name} ({', '.join(columns)})
                    VALUES %s
                    ON CONFLICT ({conflict_columns_str}) 
                    DO UPDATE SET {update_set}
                """

                # Prepare data as tuples
                values_list = [
                    tuple(None if row[col] == '' else row[col] for col in columns)
                    for row in data
                ]

                # Use execute_values for best performance with small datasets
                execute_values(cur, insert_query, values_list, page_size=100)

                conn.commit()
                logger.info(f'✅ Successfully upserted {len(data)} rows into {table_name} (fast method)')
                return True

    except (Exception, psycopg2.DatabaseError) as error:
        logger.critical(f"❌ Upsert error: {error}")
        raise




def _upsert_large_dataset(data: List[Dict[str, Any]], table_name: str, conflict_columns: set) -> bool:
    """For large datasets - uses COPY + temp table"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                columns = list(data[0].keys())
                missing_conflict_columns = conflict_columns - set(columns)
                if missing_conflict_columns:
                    raise ValueError(f"Conflict columns not found in data: {missing_conflict_columns}")

                # Create temporary table
                temp_table = f"temp_{table_name}_{uuid.uuid4().hex}"

                cur.execute(f"""
                    CREATE TEMP TABLE {temp_table} 
                    (LIKE {table_name}) 
                    ON COMMIT DROP
                """)

                # COPY data to temp table
                output = StringIO()
                writer = csv.writer(output, quoting=csv.QUOTE_MINIMAL)

                for row in data:
                    writer.writerow([None if row.get(col) == '' else row.get(col) for col in columns])
                    if row['time'] > 3000:
                        print("BAD TIME:", row['time'])
                output.seek(0)

                cur.copy_expert(
                    f"COPY {temp_table} ({', '.join(columns)}) FROM STDIN WITH CSV",
                    output
                )

                # Create index for large datasets (>10k rows)
                if len(data) > 10000 and conflict_columns:
                    index_name = f"idx_{uuid.uuid4().hex[:8]}"
                    cur.execute(f"CREATE INDEX {index_name} ON {temp_table} ({', '.join(conflict_columns)})")

                # Perform upsert
                conflict_columns_str = ', '.join(conflict_columns)
                update_columns = [col for col in columns if col not in conflict_columns]
                update_set = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])

                upsert_query = f"""
                    INSERT INTO {table_name} ({', '.join(columns)})
                    SELECT {', '.join(columns)} FROM {temp_table}
                    ON CONFLICT ({conflict_columns_str}) 
                    DO UPDATE SET {update_set}
                """

                cur.execute(upsert_query)
                conn.commit()
                logger.info(f'✅ Successfully upserted {len(data)} rows into {table_name} (bulk method)')
                return True

    except (Exception, psycopg2.DatabaseError) as error:
        logger.critical(f"❌ Upsert error: {error}")
        raise