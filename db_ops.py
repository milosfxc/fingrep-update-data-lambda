import psycopg2
import os
from typing import List, Dict, Any
import uuid
from io import StringIO
import csv
from psycopg2.extras import execute_values


def postgres_connection():
    conn = psycopg2.connect(database=os.getenv('FINGREP_POSTGRES_DATABASE'), user=os.getenv('FINGREP_POSTGRES_USER'),
                            host=os.getenv('FINGREP_POSTGRES_HOST'), port=5432,
                            password=os.getenv('FINGREP_POSTGRES_PASS'))
    return conn


def upsert_from_csv_data(csv_data: List[Dict[str, Any]], table_name: str, conflict_columns: set) -> bool:
    """
    Upsert data from CSV rows into database using ON CONFLICT DO UPDATE SET
    Supports multiple conflict columns
    """
    try:
        with postgres_connection() as conn:
            with conn.cursor() as cur:
                if not csv_data:
                    print("No data to upsert")
                    return False

                # Get column names from first row
                columns = list(csv_data[0].keys())

                # Validate that conflict columns exist in the data
                missing_conflict_columns = conflict_columns - set(columns)
                if missing_conflict_columns:
                    raise ValueError(f"Conflict columns not found in data: {missing_conflict_columns}")

                # Create the INSERT ... ON CONFLICT query dynamically
                placeholders = ', '.join(['%s'] * len(columns))

                # Handle multiple conflict columns - join with commas
                conflict_columns_str = ', '.join(conflict_columns)

                # Exclude conflict columns from update set
                update_columns = [col for col in columns if col not in conflict_columns]
                update_set = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])

                insert_query = f"""
                    INSERT INTO {table_name} ({', '.join(columns)})
                    VALUES ({placeholders})
                    ON CONFLICT ({conflict_columns_str}) 
                    DO UPDATE SET {update_set}
                """

                # Prepare data for insertion (convert values appropriately)
                values_list = []
                for row in csv_data:
                    values = []
                    for col in columns:
                        value = row[col]
                        # Handle empty strings and convert to None for NULL
                        if value == '':
                            values.append(None)
                        else:
                            values.append(value)
                    values_list.append(values)

                # Execute all inserts
                cur.executemany(insert_query, values_list)

                conn.commit()
                print(f"✅ Successfully upserted {len(csv_data)} rows into {table_name}")
                return True

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"❌ Upsert error: {error}")
        raise


def upsert_from_csv_data_smart(csv_data: List[Dict[str, Any]], table_name: str, conflict_columns: set,
                               threshold: int = 1000) -> bool:
    """
    Smart upsert that automatically chooses the best method based on data size
    """
    if not csv_data:
        print("No data to upsert")
        return False

    # Use optimized method for large datasets
    if len(csv_data) > threshold:
        return _upsert_large_dataset(csv_data, table_name, conflict_columns)
    else:
        return _upsert_small_dataset(csv_data, table_name, conflict_columns)


def _upsert_small_dataset(csv_data: List[Dict[str, Any]], table_name: str, conflict_columns: set) -> bool:
    """For small datasets - uses execute_batch"""
    try:
        with postgres_connection() as conn:
            with conn.cursor() as cur:
                columns = list(csv_data[0].keys())
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
                    for row in csv_data
                ]

                # Use execute_values for best performance with small datasets
                execute_values(cur, insert_query, values_list, page_size=100)

                conn.commit()
                print(f"✅ Successfully upserted {len(csv_data)} rows into {table_name} (fast method)")
                return True

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"❌ Upsert error: {error}")
        raise


def _upsert_large_dataset(csv_data: List[Dict[str, Any]], table_name: str, conflict_columns: set) -> bool:
    """For large datasets - uses COPY + temp table"""
    try:
        with postgres_connection() as conn:
            with conn.cursor() as cur:
                columns = list(csv_data[0].keys())
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

                for row in csv_data:
                    writer.writerow([None if row.get(col) == '' else row.get(col) for col in columns])

                output.seek(0)

                cur.copy_expert(f"COPY {temp_table} FROM STDIN WITH CSV", output)

                # Create index for large datasets (>10k rows)
                if len(csv_data) > 10000 and conflict_columns:
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
                print(f"✅ Successfully upserted {len(csv_data)} rows into {table_name} (bulk method)")
                return True

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"❌ Upsert error: {error}")
        raise