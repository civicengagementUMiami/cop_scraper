import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import json
import os
from typing import Optional, Dict, List

class MiamiDadePropertyScraper:
    def __init__(self, base_params: Optional[Dict] = None):
        self.base_url = "https://wwwx.miamidade.gov/apps/ISD/RealEstate_Portal/CountyOwnedProperties"
        self.default_params = {
            "FolioF": "",
            "PrpTypeF": "",
            "DistrictF": "",
            "LotComF": "",
            "LotF": "",
            "LocationF": "",
            "AddressF": "",
            "ZoneF": "",
            "LegalF": "",
            "SurplusF": "4",  # Default to surplus properties
            "pageIndex": 1
        }
        self.params = {**self.default_params, **(base_params or {})}
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
    def fetch_page(self, page_index: int) -> Optional[BeautifulSoup]:
        """Fetch and parse a single page"""
        self.params['pageIndex'] = page_index
        try:
            response = self.session.get(self.base_url, params=self.params, timeout=30)
            response.raise_for_status()
            return BeautifulSoup(response.text, 'html.parser')
        except requests.exceptions.RequestException as e:
            print(f"Error fetching page {page_index}: {str(e)}")
            return None
    
    def extract_table_data(self, soup: BeautifulSoup) -> tuple:
        """Extract headers and rows from table"""
        table = soup.find('table', {'class': 'table'})
        if not table:
            return None, None
            
        headers = [th.get_text(strip=True) for th in table.find('thead').find_all('th')]
        rows = []
        for row in table.find('tbody').find_all('tr'):
            cells = row.find_all('td')
            rows.append([cell.get_text(strip=True) for cell in cells])
            
        return headers, rows
    
    def scrape_all_pages(self, max_pages: int = 100, delay: float = 1.0) -> pd.DataFrame:
        """Scrape all available pages with automatic termination"""
        all_data = []
        current_page = 1
        headers = None
        consecutive_empty = 0
        max_consecutive_empty = 3  # Stop after this many empty pages
        
        print("Starting scraping session...")
        print(f"Initial parameters: {json.dumps(self.params, indent=2)}")
        
        while current_page <= max_pages:
            print(f"\nProcessing page {current_page}...", end=' ')
            
            # Fetch and parse page
            soup = self.fetch_page(current_page)
            if not soup:
                consecutive_empty += 1
                if consecutive_empty >= max_consecutive_empty:
                    print("Too many consecutive empty pages. Stopping.")
                    break
                current_page += 1
                continue
                
            # Extract data
            current_headers, current_rows = self.extract_table_data(soup)
            if not current_rows:
                print("No rows found.")
                consecutive_empty += 1
                if consecutive_empty >= max_consecutive_empty:
                    print("Too many consecutive empty pages. Stopping.")
                    break
                current_page += 1
                continue
                
            # Reset empty counter
            consecutive_empty = 0
            
            # Set/verify headers
            if headers is None:
                headers = current_headers
                print(f"Found {len(headers)} columns")
            elif current_headers != headers:
                print("Warning: Header mismatch detected!")
                
            # Store data
            all_data.extend(current_rows)
            print(f"Added {len(current_rows)} rows (Total: {len(all_data)})")
            
            # Check for termination conditions
            if len(current_rows) == 0:
                print("Reached empty page. Stopping.")
                break
                
            # Prepare for next page
            current_page += 1
            time.sleep(delay)  # Respectful delay
            
        # Create DataFrame
        if headers and all_data:
            df = pd.DataFrame(all_data, columns=headers)
            print(f"\nScraping complete. Total rows collected: {len(df)}")
            return df
        else:
            print("\nNo data collected")
            return pd.DataFrame()
    
    def save_checkpoint(self, df: pd.DataFrame, filename: str = None):
        """Save results with timestamp"""
        if filename is None:
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            filename = f"miami_dade_properties_{timestamp}.csv"
        
        # Create output directory if needed
        os.makedirs('output', exist_ok=True)
        full_path = os.path.join('output', filename)
        
        df.to_csv(full_path, index=False)
        print(f"Data saved to {full_path}")
        return full_path

def main():

    timestamp = time.strftime("%Y%m%d")
    runs = [
        {"filter": {"SurplusF": "4"}, "filename": f"{timestamp}_surplus"},
        {"filter": {"PrpTypeF": "35"}, "filename": f"{timestamp}_eel"},
        {"filter": {"PrpTypeF": "74"}, "filename": f"{timestamp}_vacant_bldg"},
        {"filter": {"PrpTypeF": "37"}, "filename": f"{timestamp}_vacant_land"},
        {"filter": {"PrpTypeF": "30"}, "filename": f"{timestamp}_cemetary"},
    ]

    for run in runs:

        # Example usage with custom parameters
        scraper = MiamiDadePropertyScraper(run["filter"])
        
        # Start scraping with:
        # - Maximum 100 pages (safety limit)
        # - 1 second delay between requests
        start_time = time.time()
        data = scraper.scrape_all_pages(max_pages=100, delay=1.0)
        
        if not data.empty:
            # Save results
            saved_file = scraper.save_checkpoint(data,f"{run['filename']}.csv")
            
            # Show summary
            print("\nScraping Summary:")
            print(f"- Total properties: {len(data)}")
            print(f"- Columns: {list(data.columns)}")
            print(f"- First Folio: {data.iloc[0,0] if len(data) > 0 else 'N/A'}")
            print(f"- Last Folio: {data.iloc[-1,0] if len(data) > 0 else 'N/A'}")
            print(f"\nExecution time: {time.time()-start_time:.2f} seconds")
            
            # Save parameters used for documentation
            with open(os.path.join('output', f"{run['filename']}_parameters.json"), 'w') as f:
                json.dump(scraper.params, f, indent=2)
        else:
            print("No data was collected")

if __name__ == "__main__":
    main()