## Summary

This repo contains sample code for the issue [https://github.com/psycopg/psycopg/issues/158](https://github.com/psycopg/psycopg/issues/158)

The issue was tested with the following setup: `macOS 12.0.1 (21A559)`
`Python 3.9.8`
`psycopg-binary 3.0.4`

The test case generates a temporary table and then tries to dump the data via `copy`.

## Environment

The following environment variables are required to be set: `PGDATABASE`, `PGUSER`, `PGPASS`, `PGHOST`, `PGPORT`.

## Usage

`python run.py --column 12 --rows 4_000_000`

adjust the `column` and `rows` parameters to your needs. Values that trigger the issue on my machine are:

columns | rows
--------|-------
12      | 4_000_000
6       | 7_000_000
3       | 14_000_000


The number open KQUEUE file descriptor can be monitored with: 

```sh
watch -n .1 'lsof -w -p <PID> | grep KQUEUE | wc -l'
```
