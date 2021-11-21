def main():
    import psycopg
    import os

    with psycopg.connect(dbname=os.getenv('PGDATABASE'),
                         user=os.getenv('PGUSER'),
                         password=os.getenv('PGPASS'),
                         host=os.getenv('PGHOST'),
                         port=os.getenv('PGPORT')) as conn:
        with conn.cursor() as cur:
            cur.execute("""
CREATE TEMP TABLE dump_test_table AS
SELECT 
    md5('00' || x::text)::text AS col_00,
    md5('01' || x::text)::text AS col_01,
    md5('02' || x::text)::text AS col_02,
    md5('03' || x::text)::text AS col_03,
    md5('04' || x::text)::text AS col_04,
    md5('05' || x::text)::text AS col_05,
    md5('06' || x::text)::text AS col_06,
    md5('07' || x::text)::text AS col_07
FROM generate_series(1, 10000000) AS x;""")

            with cur.copy("""COPY pg_temp.dump_test_table TO STDOUT;""") as copy:
                while copy.read():
                    pass


if __name__ == "__main__":
    main()
