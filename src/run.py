import argparse
import psycopg
import os
import datetime


def read_table(conn, column_count, row_count):
    time_started = datetime.datetime.now()
    print(f'\ngenerating {column_count:,} columns with {row_count:,} rows ... ', end='', flush=True)

    columns_sql = [f"""md5('{i}' || x::text)::text AS "col_{i}" """ for i in range(column_count)]
    create_sql = f"""
CREATE TEMP TABLE dump_test_table AS
SELECT 
    {', '.join(columns_sql)}
FROM generate_series(1, %s) AS x;
"""

    conn.execute(create_sql, (row_count, ))
    print(f'{datetime.datetime.now() - time_started}')

    print('dumping table ... ', end='', flush=True)
    time_started = datetime.datetime.now()

    had_error = False
    read_row_count = 0
    read_bytes_count = 0

    try:
        with conn.cursor() as cur:
            with cur.copy("""COPY pg_temp.dump_test_table TO STDOUT;""") as copy:
                while data_row := copy.read():
                    read_bytes_count += len(data_row)
                    read_row_count += 1
    except Exception as e:
        print(f'{datetime.datetime.now() - time_started}')
        print(f'{read_row_count:,} rows, {read_bytes_count:,}B')

        print(e)

        had_error = True
    else:
        conn.execute('DISCARD TEMP;')

        print(f'{datetime.datetime.now() - time_started}')
        print(f'{read_row_count:,} rows, {read_bytes_count//2**20:,}MB')

    return had_error


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--columns', default=8, type=int, help='number of columns to generate')
    parser.add_argument('--rows', default=10_000_000, type=int, help='number of rows to generate')
    args = parser.parse_args()

    print('pid: ', os.getpid())

    with psycopg.connect(dbname=os.getenv('PGDATABASE'), user=os.getenv('PGUSER'), password=os.getenv('PGPASS'), host=os.getenv('PGHOST'), port=os.getenv('PGPORT')) as conn:
        read_table(conn, args.columns, args.rows)


if __name__ == "__main__":
    main()
