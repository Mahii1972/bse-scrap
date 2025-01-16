from scrapers.bse_scraper import scrape_5paisa_bse500
from scrapers.sector_scraper import fetch_moneycontrol_sectors
from scrapers.hist_scrape import fetch_historical_market_data
from db.db import execute_query, execute_batch_query
import json
from datetime import date, datetime, timedelta
import time

def lambda_handler(event, context):
    try:
        main()
        return {
            'statusCode': 200,
            'body': json.dumps('Data scraping completed successfully')
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error occurred: {str(e)}')
        }

def main():
    try:
        # Get BSE 500 data and insert into database
        bse_data = scrape_5paisa_bse500()
        
        if "error" not in bse_data:
            bse_insert_query = """
                INSERT INTO bse500_distribution 
                (date_captured, total_companies, below_minus_15, minus_15_to_minus_10,
                minus_10_to_minus_5, minus_5_to_minus_2, minus_2_to_0, exact_0,
                plus_0_to_2, plus_2_to_5, plus_5_to_10, plus_10_to_15, above_15)
                VALUES (%(date_captured)s, %(total_companies)s, %(below_minus_15)s,
                %(minus_15_to_minus_10)s, %(minus_10_to_minus_5)s, %(minus_5_to_minus_2)s,
                %(minus_2_to_0)s, %(exact_0)s, %(plus_0_to_2)s, %(plus_2_to_5)s,
                %(plus_5_to_10)s, %(plus_10_to_15)s, %(above_15)s)
                ON CONFLICT (date_captured) DO UPDATE SET
                total_companies = EXCLUDED.total_companies,
                below_minus_15 = EXCLUDED.below_minus_15,
                minus_15_to_minus_10 = EXCLUDED.minus_15_to_minus_10,
                minus_10_to_minus_5 = EXCLUDED.minus_10_to_minus_5,
                minus_5_to_minus_2 = EXCLUDED.minus_5_to_minus_2,
                minus_2_to_0 = EXCLUDED.minus_2_to_0,
                exact_0 = EXCLUDED.exact_0,
                plus_0_to_2 = EXCLUDED.plus_0_to_2,
                plus_2_to_5 = EXCLUDED.plus_2_to_5,
                plus_5_to_10 = EXCLUDED.plus_5_to_10,
                plus_10_to_15 = EXCLUDED.plus_10_to_15,
                above_15 = EXCLUDED.above_15
            """
            if execute_query(bse_insert_query, bse_data):
                print("Successfully inserted BSE 500 data")
            else:
                print("Failed to insert BSE 500 data")
        else:
            print(f"Error occurred in BSE scraping: {bse_data['error']}")

        # Get and insert sector data for different durations
        durations = ["1d", "5d", "1m", "3m", "1y"]
        today = date.today().isoformat()
        
        for duration in durations:
            sector_data = fetch_moneycontrol_sectors(duration)
            
            # Prepare data for bulk insert
            bulk_data = []
            for sector in sector_data:
                sector['date_captured'] = today
                bulk_data.append(sector)
            
            if bulk_data:
                sector_insert_query = """
                    INSERT INTO sectoral_performance 
                    (date_captured, duration, sector, market_cap_change)
                    VALUES (%(date_captured)s, %(duration)s, %(sector)s, %(market_cap_change)s)
                    ON CONFLICT (date_captured, duration, sector) DO UPDATE SET
                    market_cap_change = EXCLUDED.market_cap_change
                """
                
                if execute_batch_query(sector_insert_query, bulk_data):
                    print(f"Successfully inserted {len(bulk_data)} sector records for {duration}")
                else:
                    print(f"Failed to insert sector data for {duration}")
            
        # Add market data scraping and insertion
        end_date = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
        start_date = "2004-01-01"  # Keep the same start date
        
        # Fetch market data for both indices
        market_data = []
        
        # Fetch NIFTY50 data
        nifty_data = fetch_historical_market_data(
            start_date=start_date,
            end_date=end_date,
            symbol_id="17940",
            index_name="NIFTY50"
        )
        if nifty_data:
            market_data.extend(nifty_data)
            time.sleep(2)  # Add delay between requests
            
        # Fetch BSE500 data
        bse_data = fetch_historical_market_data(
            start_date=start_date,
            end_date=end_date,
            symbol_id="39935",
            index_name="BSE500"
        )
        if bse_data:
            market_data.extend(bse_data)
            
        if market_data:
            market_data_query = """
                INSERT INTO market_data 
                (index_name, date, open, high, low, close, volume, change_percent)
                VALUES (%(index_name)s, %(date)s, %(open)s, %(high)s, %(low)s, 
                        %(close)s, %(volume)s, %(change_percent)s)
                ON CONFLICT (index_name, date) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                change_percent = EXCLUDED.change_percent
            """
            
            if execute_batch_query(market_data_query, market_data):
                print(f"Successfully inserted {len(market_data)} market data records")
            else:
                print("Failed to insert market data")
                
    except Exception as e:
        print(f"An error occurred in main: {str(e)}")

if __name__ == "__main__":
    main()