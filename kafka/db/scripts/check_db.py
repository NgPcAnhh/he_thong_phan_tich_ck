import asyncio
import asyncpg

async def check_database():
    try:
        conn = await asyncpg.connect(
            'postgresql://admin:123456@localhost:5432/postgres'
        )
        
        schema = 'hethong_phantich_chungkhoan'
        
        # Count total records
        count = await conn.fetchval(f'SELECT COUNT(*) FROM {schema}.realtime_quotes')
        print(f'✅ Total records in {schema}.realtime_quotes: {count:,}')
        
        if count > 0:
            # Get latest record
            row = await conn.fetchrow(f'''
                SELECT symbol, ts, last_price, total_volume 
                FROM {schema}.realtime_quotes 
                ORDER BY ts DESC 
                LIMIT 1
            ''')
            print(f'\n📊 Latest record:')
            print(f'   Symbol: {row["symbol"]}')
            print(f'   Timestamp: {row["ts"]}')
            print(f'   Last Price: {row["last_price"]}')
            print(f'   Total Volume: {row["total_volume"]}')
            
            # Get count by symbol
            rows = await conn.fetch(f'''
                SELECT symbol, COUNT(*) as cnt 
                FROM {schema}.realtime_quotes 
                GROUP BY symbol 
                ORDER BY cnt DESC 
                LIMIT 5
            ''')
            print(f'\n🔝 Top 5 symbols by record count:')
            for r in rows:
                print(f'   {r["symbol"]}: {r["cnt"]} records')
        else:
            print('\n⚠️ No records found in database')
            print('   ❌ Consumer is NOT consuming from Kafka')
            print(f'   📋 Table exists but empty: {schema}.realtime_quotes')
        
        await conn.close()
        
    except Exception as e:
        print(f'❌ Error: {e}')

if __name__ == "__main__":
    asyncio.run(check_database())
