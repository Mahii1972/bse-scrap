import cloudscraper
from datetime import datetime, timedelta
import time

def fetch_historical_market_data(start_date, end_date, time_frame="Daily", symbol_id="17940", index_name="NIFTY50"):
    url = f"https://api.investing.com/api/financialdata/historical/{symbol_id}"
    
    params = {
        "start-date": start_date,
        "end-date": end_date,
        "time-frame": time_frame,
        "add-missing-rows": "false"
    }
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:134.0) Gecko/20100101 Firefox/134.0",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Content-Type": "application/json",
        "Domain-Id": "in",
        "Origin": "https://in.investing.com",
        "Referer": "https://in.investing.com/",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site"
    }

    try:
        # Create a cloudscraper session
        scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'firefox',
                'platform': 'windows',
                'mobile': False
            }
        )
        
        # Add our custom headers
        scraper.headers.update(headers)
        
        # Add a small delay to avoid rate limiting
        time.sleep(2)
        
        # Make the request
        response = scraper.get(url, params=params)
        response.raise_for_status()
        
        data = response.json()
        
        # Format the data for database insertion
        formatted_data = []
        for item in data['data']:
            record = {
                'index_name': index_name,
                'date': datetime.strptime(item['rowDateTimestamp'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d'),
                'open': float(item['last_openRaw']),
                'high': float(item['last_maxRaw']),
                'low': float(item['last_minRaw']),
                'close': float(item['last_closeRaw']),
                'volume': int(item['volumeRaw']),
                'change_percent': float(item['change_precentRaw'])
            }
            formatted_data.append(record)
        
        return formatted_data
        
    except Exception as e:
        print(f"Error occurred: {e}")
        if hasattr(e, 'response'):
            print(f"Response content: {e.response.content}")
        return None

if __name__ == "__main__":
    # Set date range from 2004 to 2014
    start_date = "2004-01-01"
    end_date = "2014-12-31"
    
    # Fetch NIFTY50 data
    nifty_result = fetch_historical_market_data(
        start_date=start_date,
        end_date=end_date,
        symbol_id="17940",
        index_name="NIFTY50"
    )
    
    # Fetch BSE500 data
    bse_result = fetch_historical_market_data(
        start_date=start_date,
        end_date=end_date,
        symbol_id="39935",
        index_name="BSE500"
    )
