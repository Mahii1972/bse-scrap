from scrapers.bse_scraper import scrape_5paisa_bse500
from scrapers.sector_scraper import fetch_moneycontrol_sectors
from db.db import execute_query, execute_batch_query
import json
from datetime import date

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
            
    except Exception as e:
        print(f"An error occurred in main: {str(e)}")

if __name__ == "__main__":
    main()