import asyncio, psycopg_pool, re
from testcontainers.postgres import PostgresContainer

async def test():
    with PostgresContainer('postgres:16-alpine') as pg:
        url = re.sub(r'\+[^:]+(?=://)', '', pg.get_connection_url())
        pool = psycopg_pool.AsyncConnectionPool(conninfo=url, min_size=1, max_size=2, open=False)
        await pool.open()
        # Setup
        async with pool.connection() as conn:
            await conn.execute('CREATE TABLE t1 (id int, name text)')
            await conn.commit()
        
        # Test: DDL + insert + failing update + commit
        try:
            async with pool.connection() as conn:
                await conn.execute('CREATE TABLE dl_table (id bigserial primary key, val text)')
                await conn.execute("INSERT INTO dl_table (val) VALUES ('hello')")
                try:
                    await conn.execute('UPDATE t1 SET missing_col = 1 WHERE id = 1')
                except Exception as e:
                    print(f'Caught UPDATE error: {type(e).__name__}: {e}')
                try:
                    await conn.commit()
                    print('commit() succeeded')
                except Exception as e:
                    print(f'commit() failed: {type(e).__name__}: {e}')
        except Exception as e:
            print(f'Outer caught: {type(e).__name__}: {e}')
        
        # Check if dl_table was created
        async with pool.connection() as conn:
            row = await (await conn.execute("SELECT to_regclass('dl_table')")).fetchone()
        print(f'dl_table exists: {row[0]}')
        await pool.close()

asyncio.run(test())
