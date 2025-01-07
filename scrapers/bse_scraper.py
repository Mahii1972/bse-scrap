import requests
from bs4 import BeautifulSoup
from datetime import date
import json

def categorize_percentage(percentage):
    try:
        value = float(percentage.replace('%', ''))
        if value < -15:
            return "< -15%"
        elif -15 <= value < -10:
            return "-15% to -10%"
        elif -10 <= value < -5:
            return "-10% to -5%"
        elif -5 <= value < -2:
            return "-5% to -2%"
        elif -2 <= value < 0:
            return "-2% to 0%"
        elif value == 0:
            return "0%"
        elif 0 < value <= 2:
            return "0% to 2%"
        elif 2 < value <= 5:
            return "2% to 5%"
        elif 5 < value <= 10:
            return "5% to 10%"
        elif 10 < value <= 15:
            return "10% to 15%"
        else:
            return "> 15%"
    except:
        return "Invalid"

def scrape_5paisa_bse500():
    url = "https://www.5paisa.com/share-market-today/bse-500"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        stock_boxes = soup.find_all('a', class_='paisa-heatmap__box')
        
        percentage_categories = {
            "< -15%": 0,
            "-15% to -10%": 0,
            "-10% to -5%": 0,
            "-5% to -2%": 0,
            "-2% to 0%": 0,
            "0%": 0,
            "0% to 2%": 0,
            "2% to 5%": 0,
            "5% to 10%": 0,
            "10% to 15%": 0,
            "> 15%": 0,
            "Invalid": 0
        }

        for box in stock_boxes:
            percentage = box.find('div', class_='paisa__pertxt').text.strip()
            category = categorize_percentage(percentage)
            percentage_categories[category] += 1

        # Create JSON structure matching the database table
        db_data = {
            "date_captured": date.today().isoformat(),
            "total_companies": sum(percentage_categories.values()),
            "below_minus_15": percentage_categories["< -15%"],
            "minus_15_to_minus_10": percentage_categories["-15% to -10%"],
            "minus_10_to_minus_5": percentage_categories["-10% to -5%"],
            "minus_5_to_minus_2": percentage_categories["-5% to -2%"],
            "minus_2_to_0": percentage_categories["-2% to 0%"],
            "exact_0": percentage_categories["0%"],
            "plus_0_to_2": percentage_categories["0% to 2%"],
            "plus_2_to_5": percentage_categories["2% to 5%"],
            "plus_5_to_10": percentage_categories["5% to 10%"],
            "plus_10_to_15": percentage_categories["10% to 15%"],
            "above_15": percentage_categories["> 15%"]
        }
        
        return db_data

    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    result = scrape_5paisa_bse500()
    print(json.dumps(result, indent=2))