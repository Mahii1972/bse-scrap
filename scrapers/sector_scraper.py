import requests

def fetch_moneycontrol_sectors(duration):
    url = "https://api.moneycontrol.com/mcapi/v1/sector/listing"
    params = {"dur": duration, "section": "sector"}
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://www.moneycontrol.com",
        "Referer": "https://www.moneycontrol.com/"
    }
    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        return [{
            'duration': duration,
            'sector': sector_data['sector'],
            'market_cap_change': sector_data['mCapPerChange']
        } for sector_data in data['data']]
        
    except Exception as e:
        print(f"Error: {e}")
        return []

if __name__ == "__main__":
    durations = ["1d", "5d", "1m", "3m", "1y"]
    for duration in durations:
        result = fetch_moneycontrol_sectors(duration)
        print(result)