import asyncio
import json
import libsql_client
import time
from pprint import pprint as pp


class ResultObject:
    def __init__(self, result_set=None, **kwargs):
        self.__dict__.update(
            {
                "columns": None,
                "records": None,
                "last_insert_rowid": None,
                "rows_affected": None,
                "count": None,
                "error": None,
                "success": None,
            }
        )
        if kwargs:
            self.__dict__.update(kwargs)
        if result_set is not None:
            self._init_result_set(result_set)

    def _init_result_set(self, result_set):
        records = []
        for row in result_set.rows:
            record = {}
            for x, field in enumerate(row):
                record[result_set.columns[x]] = field
            records.append(record)
        self.__dict__.update(
            {
                "columns": result_set.columns,
                "records": records,
                "last_insert_rowid": result_set.last_insert_rowid,
                "rows_affected": result_set.rows_affected,
                "count": len(records),
                "success": True,
            }
        )

    def __repr__(self):
        return json.dumps(self.__dict__, indent=2)

    def __setattr__(self, name, value):
        self.__dict__[name] = value


class SQLClient:

    def __init__(self, url, *args, **kwds):
        self.url = url
        self.client = None

    async def __aenter__(self):
        self.client = libsql_client.create_client(self.url)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.client.close()

    async def execute(self, sql, args=None, debug=False):
        time_start = time.time()
        try:
            result_set = await self.client.execute(sql, args=args)
            request_duration = time.time() - time_start
        except libsql_client.client.LibsqlError as ex:
            return ResultObject(
                success=False, error={"message": ex.explanation, "code": ex.code}
            )
        start_serialization = time.time()
        obj = ResultObject(result_set=result_set)
        obj.request_duration = request_duration
        obj.serialization_duration = time.time() - start_serialization
        obj.total_duration = time.time() - time_start
        return obj


class Bench:
    def __init__(self, title=None):
        self.title = title
        self.time_start = None
        self.time_end = None
        self.duration = None

    def reset(self):
        self.time_end = None
        self._duration = None
        self.time_start = time.time()

    def __enter__(self):
        self.time_start = time.time()
        print(f"Benching {self.title}.")
        return self

    @property
    def duration(self):
        if not self._duration:
            self.time_end = time.time()
            self._duration = time.time() - self.time_start
        return self._duration

    @duration.setter
    def duration(self, value):
        self._duration = value

    def __exit__(self, *args):
        self.duration
        print(f"-> {self.title} took {self.duration.__round__(3)} seconds.")


async def execute_bench(url, record_count=5000):
    bench_start = time.time()

    async with SQLClient(url) as client:

        # Drop table
        if (await client.execute("drop table pony;")).success:
            print(
                "Table pony is deleted. Crashed previous time? Clean up didn't happen."
            )
        else:
            print("Table pony does not exist. That's expected.")

        # Create table
        with Bench("create table pony"):
            await client.execute(
                "create table pony (id INTEGER PRIMARY KEY AUTOINCREMENT, name, age, color)"
            )

        # Success is False, table already exists
        with Bench("create table pony again. Should fail to pass"):
            assert (
                await client.execute("create table pony (id, name, age, color)")
            ).success is False

        # Check if table and sequence table is created, count should equal two.
        # This part can break in case of server meaning there's delay in
        # schema synchronization
        with Bench("checking if schema is updated"):
            while (await client.execute("SELECT * from sqlite_schema")).count != 2:
                print("Schema not synchronized. Sleeping a second before retyry.")
                await asyncio.sleep(1)

        # Prepare {record_count} amount of tasks. Default 5000
        tasks = []
        with Bench(
            f"preparing insertion of {record_count} records by creating {record_count} tasks with insert statement."
        ):
            for x in range(record_count):
                tasks.append(
                    client.execute(
                        "insert into pony(name, age, color) VALUES (?,?,?)",
                        ("Pony" + str(x), x * 1337, "blue"),
                    )
                )

        # Execute all tasks async in parallel
        duration_insert = None
        with Bench("Executing {} tasks in parallel.".format(record_count)) as bench:
            await asyncio.gather(*tasks)
            duration_insert = bench.duration

        time_waited = 0
        with Bench("waiting until all inserted data is available for read") as bench:
            while True:
                records_inserted = (
                    await client.execute("SELECT count(0) as c from pony", debug=True)
                ).records[0]["c"]
                print(
                    "Records insert check: {}/{}".format(records_inserted, record_count)
                )
                if records_inserted == record_count:
                    break
                await asyncio.sleep(2)
            time_waited = bench.duration

        with Bench("cleaning up by dropping table pony"):
            await client.execute("drop table pony;")

        return json.dumps(
            dict(
                duration_syncing=time_waited,
                duration_insert=duration_insert,
                duration_total=time.time() - bench_start,
                duration_total_without_waiting=time.time() - bench_start - time_waited,
            ),
            indent=2,
        )


async def async_test(url):
    async with SQLClient(url) as client:
        await client.execute(
            "create table insert_test (id INTEGER PRIMARY KEY AUTOINCREMENT, description)"
        )
        await client.execute(
            "insert into insert_test(description) VALUES (?)", ("test",)
        )
        result = (await client.execute("select * from insert_test")).count == 1
        await client.execute("drop table insert_test")
        return result


async def main():

    url_server = "http://localhost:8080"
    url_file = "file:local.db"

    # Conclusion of this test: most of the time,
    # a just single inserted record is not directly
    # to read from server
    with Bench("checks synchronization speed server"):
        for x in range(1, 11):
            print(
                "Check {} if inserted record to server is directy available: {}".format(
                    x, await async_test(url_server)
                )
            )
    input("Press enter to continue")

    # Conclusion of this test: always, a just single
    # inserted record is directly readable from the file
    with Bench("checks synchronization speed file"):
        for x in range(1, 11):
            print(
                "Check {} if inserted record to file is directy available: {}".format(
                    x, await async_test(url_file)
                )
            )
    input("Press enter to continue")

    # We're gonna put 5000 records in the database using the server
    duration_server = await execute_bench(url_server)
    print(f"server: {duration_server}")
    # We're gonna put 5000 records in the database using the file
    duration_file = await execute_bench(url_file)
    print("TOTAL RESULTS:")
    print(f"server: {duration_server}")
    print(f"file: {duration_file}")


asyncio.run(main())
