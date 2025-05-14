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
            "SurplusF": "", 
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
    
    def save_checkpoint(self, df: pd.DataFrame, path: str = "output", filename: str = None):
        """Save results with timestamp"""
        if filename is None:
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            filename = f"miami_dade_properties_{timestamp}.csv"
        
        # Create output directory if needed
        os.makedirs(path, exist_ok=True)
        full_path = os.path.join(path, filename)
        
        df.to_csv(full_path, index=False)
        print(f"Data saved to {full_path}")

        df.to_csv(f"{path}/{path}.csv", columns=['Folio','source'], index=False, mode='a')
        return full_path

def main():

    timestamp = time.strftime("%Y%m%d")
    actions = {
        "exclude": [
            {"filter": {"PrpTypeF": "42"}, "filename": f"{timestamp}_AffordableHousingRentals"},
            {"filter": {"PrpTypeF": "72"}, "filename": f"{timestamp}_AnimalShelter"},
            {"filter": {"PrpTypeF": "94"}, "filename": f"{timestamp}_Antenna"},
            {"filter": {"PrpTypeF": "75"}, "filename": f"{timestamp}_Busway"},
            {"filter": {"PrpTypeF": "84"}, "filename": f"{timestamp}_Canal"},
            {"filter": {"PrpTypeF": "30"}, "filename": f"{timestamp}_Cemetery"},
            {"filter": {"PrpTypeF": "54"}, "filename": f"{timestamp}_ChillerPlant"},
            {"filter": {"PrpTypeF": "56"}, "filename": f"{timestamp}_CivicCenter"},
            {"filter": {"PrpTypeF": "50"}, "filename": f"{timestamp}_CommonArea"},
            {"filter": {"PrpTypeF": "90"}, "filename": f"{timestamp}_CooperativeExtension"},
            {"filter": {"PrpTypeF": "26"}, "filename": f"{timestamp}_CorrectionalFacility"},
            {"filter": {"PrpTypeF": "49"}, "filename": f"{timestamp}_Courts"},
            {"filter": {"PrpTypeF": "53"}, "filename": f"{timestamp}_EnvironmentallyEndangeredLandsEel"},
            {"filter": {"PrpTypeF": "1"}, "filename": f"{timestamp}_EnvironmentallySensitiveNonEel"},
            {"filter": {"PrpTypeF": "18"}, "filename": f"{timestamp}_FireStation"},
            {"filter": {"PrpTypeF": "55"}, "filename": f"{timestamp}_FleetShop"},
            {"filter": {"PrpTypeF": "26"}, "filename": f"{timestamp}_FplEasement"},
            {"filter": {"PrpTypeF": "92"}, "filename": f"{timestamp}_Gymnasium"},
            {"filter": {"PrpTypeF": "60"}, "filename": f"{timestamp}_HeadStartCenter"},
            {"filter": {"PrpTypeF": "57"}, "filename": f"{timestamp}_HealthClinic"},
            {"filter": {"PrpTypeF": "73"}, "filename": f"{timestamp}_HomelessShelter"},
            {"filter": {"PrpTypeF": "69"}, "filename": f"{timestamp}_HomesteadGeneralAirport"},
            {"filter": {"PrpTypeF": "93"}, "filename": f"{timestamp}_Hotel"},
            {"filter": {"PrpTypeF": "28"}, "filename": f"{timestamp}_ImprovedProperty"},
            {"filter": {"PrpTypeF": "59"}, "filename": f"{timestamp}_InspectionStation"},
            {"filter": {"PrpTypeF": "4"}, "filename": f"{timestamp}_Lake"},
            {"filter": {"PrpTypeF": "19"}, "filename": f"{timestamp}_Landfill"},
            {"filter": {"PrpTypeF": "77"}, "filename": f"{timestamp}_Landmark"},
            {"filter": {"PrpTypeF": "25"}, "filename": f"{timestamp}_Library"},
            {"filter": {"PrpTypeF": "78"}, "filename": f"{timestamp}_LiftStation"},
            {"filter": {"PrpTypeF": "61"}, "filename": f"{timestamp}_MaintenanceFacility"},
            {"filter": {"PrpTypeF": "53"}, "filename": f"{timestamp}_MetromoverStation"},
            {"filter": {"PrpTypeF": "98"}, "filename": f"{timestamp}_Metrorail"},
            {"filter": {"PrpTypeF": "39"}, "filename": f"{timestamp}_MetrorailStation"},
            {"filter": {"PrpTypeF": "51"}, "filename": f"{timestamp}_MiamiIntermodalCenter"},
            {"filter": {"PrpTypeF": "66"}, "filename": f"{timestamp}_MiamiInternationalAirport"},
            {"filter": {"PrpTypeF": "34"}, "filename": f"{timestamp}_Museum"},
            {"filter": {"PrpTypeF": "22"}, "filename": f"{timestamp}_NeighborhoodServiceCenter"},
            {"filter": {"PrpTypeF": "96"}, "filename": f"{timestamp}_OfficeWarehouse"},
            {"filter": {"PrpTypeF": "70"}, "filename": f"{timestamp}_OpaLockaAirport"},
            {"filter": {"PrpTypeF": "71"}, "filename": f"{timestamp}_OpaLockaWestAirport"},
            {"filter": {"PrpTypeF": "14"}, "filename": f"{timestamp}_Park"},
            {"filter": {"PrpTypeF": "99"}, "filename": f"{timestamp}_ParkAndRide"},
            {"filter": {"PrpTypeF": "48"}, "filename": f"{timestamp}_ParkingGarage"},
            {"filter": {"PrpTypeF": "62"}, "filename": f"{timestamp}_PerformingArtsCenter"},
            {"filter": {"PrpTypeF": "100"}, "filename": f"{timestamp}_PoliceTrainingFacility"},
            {"filter": {"PrpTypeF": "47"}, "filename": f"{timestamp}_PublicHousing"},
            {"filter": {"PrpTypeF": "45"}, "filename": f"{timestamp}_PumpStation"},
            {"filter": {"PrpTypeF": "91"}, "filename": f"{timestamp}_RecreationFacility"},
            {"filter": {"PrpTypeF": "7"}, "filename": f"{timestamp}_RightOfWay"},
            {"filter": {"PrpTypeF": "9"}, "filename": f"{timestamp}_Rockpit"},
            {"filter": {"PrpTypeF": "87"}, "filename": f"{timestamp}_School"},
            {"filter": {"PrpTypeF": "46"}, "filename": f"{timestamp}_Seaport"},
            {"filter": {"PrpTypeF": "101"}, "filename": f"{timestamp}_Shelter"},
            {"filter": {"PrpTypeF": "10"}, "filename": f"{timestamp}_Sliver"},
            {"filter": {"PrpTypeF": "38"}, "filename": f"{timestamp}_SolidWasteFacility"},
            {"filter": {"PrpTypeF": "29"}, "filename": f"{timestamp}_SpecialTaxingDistrict"},
            {"filter": {"PrpTypeF": "44"}, "filename": f"{timestamp}_Stadium"},
            {"filter": {"PrpTypeF": "79"}, "filename": f"{timestamp}_StormWaterRetention"},
            {"filter": {"PrpTypeF": "63"}, "filename": f"{timestamp}_SubmergedLands"},
            {"filter": {"PrpTypeF": "68"}, "filename": f"{timestamp}_TamiamiAirport"},
            {"filter": {"PrpTypeF": "64"}, "filename": f"{timestamp}_Trash&RecyclingCenter"},
            {"filter": {"PrpTypeF": "15"}, "filename": f"{timestamp}_TreatmentPlant"},
            {"filter": {"PrpTypeF": "65"}, "filename": f"{timestamp}_Warehouse"},
            {"filter": {"PrpTypeF": "88"}, "filename": f"{timestamp}_WaterPlant"},
            {"filter": {"PrpTypeF": "23"}, "filename": f"{timestamp}_Wellfield"}
        ],
        "include": [
            {"filter": {"PrpTypeF": "74"}, "filename": f"{timestamp}_VacantBuilding"},
            {"filter": {"PrpTypeF": "37", "SurplusF": "22"}, "filename": f"{timestamp}_VacantLand"},
            {"filter": {"PrpTypeF": "40", "SurplusF": "22"}, "filename": f"{timestamp}_GovernmentCenter"},
            {"filter": {"PrpTypeF": "21", "SurplusF": "22"}, "filename": f"{timestamp}_OfficeBuilding"}
        ]
    }

    for action in actions:
        for condition in actions[action]:

            # Example usage with custom parameters
            scraper = MiamiDadePropertyScraper(condition['filter'])
            
            # Start scraping with:
            # - Maximum 100 pages (safety limit)
            # - 1 second delay between requests
            start_time = time.time()
            data = scraper.scrape_all_pages(max_pages=100, delay=1.0)
            
            if not data.empty:
                # Save results
                data['source'] = condition['filename'][9:]
                saved_file = scraper.save_checkpoint(data,path=action,filename=f"{condition['filename']}.csv")
                
                # Show summary
                summary = {
                    "title": f"Scraping Summary for {condition['filename']}",
                    "total properties": f"{len(data)}",
                    "columns": f"{list(data.columns)}",
                    "first folio": f"{data.iloc[0,0] if len(data) > 0 else 'N/A'}",
                    "last folio": f"{data.iloc[-1,0] if len(data) > 0 else 'N/A'}",
                    "execution time": f"{time.time()-start_time:.2f} seconds"
                }
                print(json.dumps(summary, indent=2))
              
                # Save parameters used and summary for documentation
                docs = {
                    "parameters": scraper.params,
                    "summary": summary
                }
                with open(os.path.join(action, f"{condition['filename']}_parameters.json"), 'w') as f:
                    json.dump(docs, f, indent=2)

            else:
                print("No data was collected")

if __name__ == "__main__":
    main()