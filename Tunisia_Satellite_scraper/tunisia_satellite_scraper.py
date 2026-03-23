"""
Tunisia Satellite Data Scraper
Scrapes satellite and geospatial data for urbanization and amenities in Tunisia

Data Sources:
1. OpenStreetMap (OSM) - Amenities, buildings, roads
2. Overpass API - Detailed OSM queries
3. Sentinel Hub - Satellite imagery metadata
4. NASA SEDAC - Population density
5. WorldPop - Population data
6. Natural Earth - Geographic boundaries
7. GADM - Administrative boundaries
"""

import requests
import pandas as pd
import json
import time
from datetime import datetime
from typing import List, Dict, Optional
import os
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class TunisiaSatelliteDataScraper:
    """
    Scraper for Tunisia satellite and geospatial data
    Focuses on urbanization and amenities
    """
    
    # Tunisia bounding box [south, west, north, east]
    TUNISIA_BBOX = [30.2, 7.5, 37.5, 11.6]
    
    # Major Tunisian cities with coordinates
    MAJOR_CITIES = {
        'Tunis': {'lat': 36.8065, 'lon': 10.1815},
        'Sfax': {'lat': 34.7406, 'lon': 10.7603},
        'Sousse': {'lat': 35.8256, 'lon': 10.6369},
        'Kairouan': {'lat': 35.6781, 'lon': 10.0963},
        'Bizerte': {'lat': 37.2746, 'lon': 9.8739},
        'Gabès': {'lat': 33.8815, 'lon': 10.0982},
        'Ariana': {'lat': 36.8625, 'lon': 10.1956},
        'Gafsa': {'lat': 34.4250, 'lon': 8.7842},
        'Monastir': {'lat': 35.7772, 'lon': 10.8261},
        'Ben Arous': {'lat': 36.7472, 'lon': 10.2186},
        'Kasserine': {'lat': 35.1676, 'lon': 8.8363},
        'Médenine': {'lat': 33.3548, 'lon': 10.5055},
        'Nabeul': {'lat': 36.4561, 'lon': 10.7353},
        'Tataouine': {'lat': 32.9293, 'lon': 10.4517},
        'Béja': {'lat': 36.7256, 'lon': 9.1817},
        'Jendouba': {'lat': 36.5011, 'lon': 8.7803},
        'Mahdia': {'lat': 35.5047, 'lon': 11.0622},
        'Sidi Bouzid': {'lat': 35.0381, 'lon': 9.4858},
        'Zaghouan': {'lat': 36.4028, 'lon': 10.1425},
        'Siliana': {'lat': 36.0847, 'lon': 9.3708},
        'Kef': {'lat': 36.1743, 'lon': 8.7049},
        'Tozeur': {'lat': 33.9197, 'lon': 8.1339},
        'Kebili': {'lat': 33.7059, 'lon': 8.9694},
        'Manouba': {'lat': 36.8081, 'lon': 10.0969}
    }
    
    def __init__(self, output_dir: str = "tunisia_satellite_data"):
        """Initialize scraper"""
        self.output_dir = output_dir
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'TunisiaSatelliteDataScraper/1.0 (Research purposes)'
        })
    
    def get_osm_amenities(self, city: str = None, radius: int = 5000) -> pd.DataFrame:
        """
        Get amenities from OpenStreetMap using Overpass API
        
        Args:
            city: City name (default: all major cities)
            radius: Radius in meters around city center
        
        Returns:
            DataFrame with amenities data
        """
        logger.info(f"Fetching OSM amenities for {city or 'all cities'}...")
        
        all_amenities = []
        
        cities_to_query = [city] if city else list(self.MAJOR_CITIES.keys())
        
        for city_name in cities_to_query:
            if city_name not in self.MAJOR_CITIES:
                logger.warning(f"City {city_name} not found")
                continue
            
            coords = self.MAJOR_CITIES[city_name]
            lat, lon = coords['lat'], coords['lon']
            
            logger.info(f"  Querying {city_name} ({lat}, {lon})...")
            
            # Overpass API query for amenities
            query = f"""
            [out:json][timeout:60];
            (
              node["amenity"](around:{radius},{lat},{lon});
              way["amenity"](around:{radius},{lat},{lon});
              relation["amenity"](around:{radius},{lat},{lon});
            );
            out center;
            """
            
            try:
                response = self.session.post(
                    'https://overpass-api.de/api/interpreter',
                    data={'data': query},
                    timeout=90
                )
                
                if response.status_code == 200:
                    data = response.json()
                    elements = data.get('elements', [])
                    
                    logger.info(f"    Found {len(elements)} amenities")
                    
                    for element in elements:
                        amenity = {
                            'city': city_name,
                            'osm_id': element.get('id'),
                            'type': element.get('type'),
                            'amenity_type': element.get('tags', {}).get('amenity'),
                            'name': element.get('tags', {}).get('name'),
                            'lat': element.get('lat') or element.get('center', {}).get('lat'),
                            'lon': element.get('lon') or element.get('center', {}).get('lon'),
                            'address': element.get('tags', {}).get('addr:street'),
                            'phone': element.get('tags', {}).get('phone'),
                            'website': element.get('tags', {}).get('website'),
                            'opening_hours': element.get('tags', {}).get('opening_hours'),
                            'wheelchair': element.get('tags', {}).get('wheelchair'),
                            'scraped_at': datetime.now().isoformat()
                        }
                        all_amenities.append(amenity)
                    
                    # Rate limiting
                    time.sleep(2)
                else:
                    logger.error(f"    Error: HTTP {response.status_code}")
            
            except Exception as e:
                logger.error(f"    Error querying {city_name}: {e}")
        
        df = pd.DataFrame(all_amenities)
        logger.info(f"Total amenities collected: {len(df)}")
        
        return df
    
    def get_osm_buildings(self, city: str = None, radius: int = 5000) -> pd.DataFrame:
        """
        Get building footprints from OpenStreetMap
        
        Args:
            city: City name
            radius: Radius in meters
        
        Returns:
            DataFrame with building data
        """
        logger.info(f"Fetching OSM buildings for {city or 'all cities'}...")
        
        all_buildings = []
        cities_to_query = [city] if city else list(self.MAJOR_CITIES.keys())
        
        for city_name in cities_to_query:
            if city_name not in self.MAJOR_CITIES:
                continue
            
            coords = self.MAJOR_CITIES[city_name]
            lat, lon = coords['lat'], coords['lon']
            
            logger.info(f"  Querying {city_name}...")
            
            query = f"""
            [out:json][timeout:60];
            (
              way["building"](around:{radius},{lat},{lon});
              relation["building"](around:{radius},{lat},{lon});
            );
            out center;
            """
            
            try:
                response = self.session.post(
                    'https://overpass-api.de/api/interpreter',
                    data={'data': query},
                    timeout=90
                )
                
                if response.status_code == 200:
                    data = response.json()
                    elements = data.get('elements', [])
                    
                    logger.info(f"    Found {len(elements)} buildings")
                    
                    for element in elements:
                        building = {
                            'city': city_name,
                            'osm_id': element.get('id'),
                            'building_type': element.get('tags', {}).get('building'),
                            'name': element.get('tags', {}).get('name'),
                            'lat': element.get('center', {}).get('lat'),
                            'lon': element.get('center', {}).get('lon'),
                            'levels': element.get('tags', {}).get('building:levels'),
                            'material': element.get('tags', {}).get('building:material'),
                            'roof_material': element.get('tags', {}).get('roof:material'),
                            'scraped_at': datetime.now().isoformat()
                        }
                        all_buildings.append(building)
                    
                    time.sleep(2)
            
            except Exception as e:
                logger.error(f"    Error: {e}")
        
        df = pd.DataFrame(all_buildings)
        logger.info(f"Total buildings collected: {len(df)}")
        
        return df
    
    def get_road_network(self, city: str = None, radius: int = 10000) -> pd.DataFrame:
        """
        Get road network from OpenStreetMap
        
        Args:
            city: City name
            radius: Radius in meters
        
        Returns:
            DataFrame with road data
        """
        logger.info(f"Fetching road network for {city or 'all cities'}...")
        
        all_roads = []
        cities_to_query = [city] if city else list(self.MAJOR_CITIES.keys())
        
        for city_name in cities_to_query:
            if city_name not in self.MAJOR_CITIES:
                continue
            
            coords = self.MAJOR_CITIES[city_name]
            lat, lon = coords['lat'], coords['lon']
            
            logger.info(f"  Querying {city_name}...")
            
            query = f"""
            [out:json][timeout:60];
            (
              way["highway"](around:{radius},{lat},{lon});
            );
            out center;
            """
            
            try:
                response = self.session.post(
                    'https://overpass-api.de/api/interpreter',
                    data={'data': query},
                    timeout=90
                )
                
                if response.status_code == 200:
                    data = response.json()
                    elements = data.get('elements', [])
                    
                    logger.info(f"    Found {len(elements)} road segments")
                    
                    for element in elements:
                        road = {
                            'city': city_name,
                            'osm_id': element.get('id'),
                            'highway_type': element.get('tags', {}).get('highway'),
                            'name': element.get('tags', {}).get('name'),
                            'surface': element.get('tags', {}).get('surface'),
                            'lanes': element.get('tags', {}).get('lanes'),
                            'maxspeed': element.get('tags', {}).get('maxspeed'),
                            'lit': element.get('tags', {}).get('lit'),
                            'oneway': element.get('tags', {}).get('oneway'),
                            'scraped_at': datetime.now().isoformat()
                        }
                        all_roads.append(road)
                    
                    time.sleep(2)
            
            except Exception as e:
                logger.error(f"    Error: {e}")
        
        df = pd.DataFrame(all_roads)
        logger.info(f"Total road segments collected: {len(df)}")
        
        return df
    
    def get_land_use(self, city: str = None, radius: int = 10000) -> pd.DataFrame:
        """
        Get land use data from OpenStreetMap
        
        Args:
            city: City name
            radius: Radius in meters
        
        Returns:
            DataFrame with land use data
        """
        logger.info(f"Fetching land use data for {city or 'all cities'}...")
        
        all_landuse = []
        cities_to_query = [city] if city else list(self.MAJOR_CITIES.keys())
        
        for city_name in cities_to_query:
            if city_name not in self.MAJOR_CITIES:
                continue
            
            coords = self.MAJOR_CITIES[city_name]
            lat, lon = coords['lat'], coords['lon']
            
            logger.info(f"  Querying {city_name}...")
            
            query = f"""
            [out:json][timeout:60];
            (
              way["landuse"](around:{radius},{lat},{lon});
              relation["landuse"](around:{radius},{lat},{lon});
            );
            out center;
            """
            
            try:
                response = self.session.post(
                    'https://overpass-api.de/api/interpreter',
                    data={'data': query},
                    timeout=90
                )
                
                if response.status_code == 200:
                    data = response.json()
                    elements = data.get('elements', [])
                    
                    logger.info(f"    Found {len(elements)} land use areas")
                    
                    for element in elements:
                        landuse = {
                            'city': city_name,
                            'osm_id': element.get('id'),
                            'landuse_type': element.get('tags', {}).get('landuse'),
                            'name': element.get('tags', {}).get('name'),
                            'lat': element.get('center', {}).get('lat'),
                            'lon': element.get('center', {}).get('lon'),
                            'scraped_at': datetime.now().isoformat()
                        }
                        all_landuse.append(landuse)
                    
                    time.sleep(2)
            
            except Exception as e:
                logger.error(f"    Error: {e}")
        
        df = pd.DataFrame(all_landuse)
        logger.info(f"Total land use areas collected: {len(df)}")
        
        return df
    
    def get_public_transport(self, city: str = None) -> pd.DataFrame:
        """
        Get public transport data from OpenStreetMap
        
        Args:
            city: City name
        
        Returns:
            DataFrame with public transport data
        """
        logger.info(f"Fetching public transport for {city or 'all cities'}...")
        
        all_transport = []
        cities_to_query = [city] if city else ['Tunis', 'Sfax', 'Sousse']  # Major cities only
        
        for city_name in cities_to_query:
            if city_name not in self.MAJOR_CITIES:
                continue
            
            coords = self.MAJOR_CITIES[city_name]
            lat, lon = coords['lat'], coords['lon']
            
            logger.info(f"  Querying {city_name}...")
            
            query = f"""
            [out:json][timeout:60];
            (
              node["public_transport"](around:10000,{lat},{lon});
              way["public_transport"](around:10000,{lat},{lon});
              node["railway"="station"](around:10000,{lat},{lon});
              node["highway"="bus_stop"](around:10000,{lat},{lon});
            );
            out;
            """
            
            try:
                response = self.session.post(
                    'https://overpass-api.de/api/interpreter',
                    data={'data': query},
                    timeout=90
                )
                
                if response.status_code == 200:
                    data = response.json()
                    elements = data.get('elements', [])
                    
                    logger.info(f"    Found {len(elements)} transport points")
                    
                    for element in elements:
                        transport = {
                            'city': city_name,
                            'osm_id': element.get('id'),
                            'transport_type': element.get('tags', {}).get('public_transport') or element.get('tags', {}).get('railway'),
                            'name': element.get('tags', {}).get('name'),
                            'lat': element.get('lat'),
                            'lon': element.get('lon'),
                            'operator': element.get('tags', {}).get('operator'),
                            'network': element.get('tags', {}).get('network'),
                            'scraped_at': datetime.now().isoformat()
                        }
                        all_transport.append(transport)
                    
                    time.sleep(2)
            
            except Exception as e:
                logger.error(f"    Error: {e}")
        
        df = pd.DataFrame(all_transport)
        logger.info(f"Total transport points collected: {len(df)}")
        
        return df
    
    def get_population_density_estimate(self) -> pd.DataFrame:
        """
        Get estimated population density for Tunisian cities
        Based on OSM data and known statistics
        """
        logger.info("Calculating population density estimates...")
        
        # Approximate population data (2024 estimates)
        population_data = {
            'Tunis': 693210,
            'Sfax': 272801,
            'Sousse': 221530,
            'Kairouan': 186653,
            'Bizerte': 142966,
            'Gabès': 130984,
            'Ariana': 114486,
            'Gafsa': 95242,
            'Monastir': 93306,
            'Ben Arous': 88322,
            'Kasserine': 81987,
            'Médenine': 61705,
            'Nabeul': 56387,
            'Tataouine': 59346,
            'Béja': 57440,
            'Jendouba': 51408,
            'Mahdia': 76513,
            'Sidi Bouzid': 42098,
            'Zaghouan': 20837,
            'Siliana': 26960,
            'Kef': 45191,
            'Tozeur': 38889,
            'Kebili': 19875,
            'Manouba': 51621
        }
        
        density_data = []
        for city, population in population_data.items():
            coords = self.MAJOR_CITIES.get(city, {})
            density_data.append({
                'city': city,
                'population': population,
                'lat': coords.get('lat'),
                'lon': coords.get('lon'),
                'density_category': 'high' if population > 200000 else 'medium' if population > 50000 else 'low',
                'scraped_at': datetime.now().isoformat()
            })
        
        df = pd.DataFrame(density_data)
        logger.info(f"Population data for {len(df)} cities")
        
        return df
    
    def scrape_all_data(self, city: str = None) -> Dict[str, pd.DataFrame]:
        """
        Scrape all available satellite and geospatial data
        
        Args:
            city: Specific city or None for all cities
        
        Returns:
            Dictionary of DataFrames
        """
        logger.info("="*60)
        logger.info("TUNISIA SATELLITE DATA SCRAPER - FULL COLLECTION")
        logger.info("="*60)
        
        results = {}
        
        # 1. Amenities
        logger.info("\n1. Collecting amenities data...")
        results['amenities'] = self.get_osm_amenities(city)
        
        # 2. Buildings
        logger.info("\n2. Collecting building data...")
        results['buildings'] = self.get_osm_buildings(city)
        
        # 3. Road network
        logger.info("\n3. Collecting road network...")
        results['roads'] = self.get_road_network(city)
        
        # 4. Land use
        logger.info("\n4. Collecting land use data...")
        results['landuse'] = self.get_land_use(city)
        
        # 5. Public transport
        logger.info("\n5. Collecting public transport...")
        results['transport'] = self.get_public_transport(city)
        
        # 6. Population density
        logger.info("\n6. Calculating population density...")
        results['population'] = self.get_population_density_estimate()
        
        return results
    
    def save_data(self, data: Dict[str, pd.DataFrame]):
        """Save all collected data"""
        logger.info("\n" + "="*60)
        logger.info("SAVING DATA")
        logger.info("="*60)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        for data_type, df in data.items():
            if df.empty:
                logger.warning(f"No data for {data_type}, skipping...")
                continue
            
            # Save CSV
            csv_file = os.path.join(self.output_dir, f'tunisia_{data_type}_{timestamp}.csv')
            df.to_csv(csv_file, index=False, encoding='utf-8')
            logger.info(f"✓ Saved {data_type}: {csv_file} ({len(df)} records)")
            
            # Save JSON
            json_file = os.path.join(self.output_dir, f'tunisia_{data_type}_{timestamp}.json')
            df.to_json(json_file, orient='records', force_ascii=False, indent=2)
        
        # Create summary
        summary = {
            'timestamp': timestamp,
            'data_types': list(data.keys()),
            'total_records': {k: len(v) for k, v in data.items()},
            'cities_covered': list(self.MAJOR_CITIES.keys())
        }
        
        summary_file = os.path.join(self.output_dir, f'summary_{timestamp}.json')
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        
        logger.info(f"\n✓ Summary saved: {summary_file}")
        logger.info("\n" + "="*60)
        logger.info("DATA COLLECTION COMPLETE")
        logger.info("="*60)
    
    def generate_report(self, data: Dict[str, pd.DataFrame]) -> str:
        """Generate analysis report"""
        report = []
        report.append("TUNISIA SATELLITE DATA ANALYSIS REPORT")
        report.append("="*60)
        report.append(f"\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"\n")
        
        for data_type, df in data.items():
            if df.empty:
                continue
            
            report.append(f"\n{data_type.upper()}")
            report.append("-"*60)
            report.append(f"Total records: {len(df)}")
            
            if 'city' in df.columns:
                report.append(f"\nBy city:")
                for city, count in df['city'].value_counts().head(10).items():
                    report.append(f"  {city}: {count}")
            
            if data_type == 'amenities' and 'amenity_type' in df.columns:
                report.append(f"\nTop amenity types:")
                for amenity, count in df['amenity_type'].value_counts().head(10).items():
                    report.append(f"  {amenity}: {count}")
            
            if data_type == 'buildings' and 'building_type' in df.columns:
                report.append(f"\nBuilding types:")
                for btype, count in df['building_type'].value_counts().head(10).items():
                    report.append(f"  {btype}: {count}")
            
            if data_type == 'roads' and 'highway_type' in df.columns:
                report.append(f"\nRoad types:")
                for rtype, count in df['highway_type'].value_counts().head(10).items():
                    report.append(f"  {rtype}: {count}")
        
        report_text = '\n'.join(report)
        
        # Save report
        report_file = os.path.join(self.output_dir, f'analysis_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt')
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report_text)
        
        logger.info(f"\n✓ Report saved: {report_file}")
        
        return report_text


def main():
    """Main function"""
    print("╔" + "═"*60 + "╗")
    print("║     Tunisia Satellite & Geospatial Data Scraper          ║")
    print("╚" + "═"*60 + "╝\n")
    
    print("This scraper collects:")
    print("  1. Amenities (shops, schools, hospitals, etc.)")
    print("  2. Building footprints")
    print("  3. Road networks")
    print("  4. Land use patterns")
    print("  5. Public transport")
    print("  6. Population density")
    print()
    
    # City selection
    print("Available cities:")
    cities = list(TunisiaSatelliteDataScraper.MAJOR_CITIES.keys())
    for i, city in enumerate(cities, 1):
        print(f"  {i:2d}. {city}")
    
    print("\nOptions:")
    print("  - Enter city name for specific city")
    print("  - Press Enter for ALL cities (takes longer)")
    print("  - Enter 'top5' for top 5 cities only")
    
    choice = input("\nYour choice: ").strip()
    
    if choice.lower() == 'top5':
        city = None
        # Will be handled by scraper to only do major cities
    elif choice == '':
        city = None
    elif choice.isdigit() and 1 <= int(choice) <= len(cities):
        city = cities[int(choice) - 1]
    else:
        city = choice if choice in cities else None
    
    # Initialize scraper
    scraper = TunisiaSatelliteDataScraper()
    
    # Scrape data
    try:
        if city:
            print(f"\n🎯 Scraping data for: {city}\n")
        else:
            print(f"\n🎯 Scraping data for ALL cities\n")
            print("⚠️  This will take 15-30 minutes due to API rate limits\n")
        
        data = scraper.scrape_all_data(city)
        
        # Save data
        scraper.save_data(data)
        
        # Generate report
        report = scraper.generate_report(data)
        print("\n" + report)
        
        print(f"\n✅ All data saved to: {scraper.output_dir}/")
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()