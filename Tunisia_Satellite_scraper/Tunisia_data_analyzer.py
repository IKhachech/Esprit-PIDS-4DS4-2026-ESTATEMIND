"""
Tunisia Satellite Data Analyzer
Analyzes and visualizes urbanization and amenities data
"""

import pandas as pd
import json
import os
from typing import Dict, List
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns
from collections import Counter


class TunisiaDataAnalyzer:
    """Analyze Tunisia satellite and geospatial data"""
    
    def __init__(self, data_dir: str = "tunisia_satellite_data"):
        """Initialize analyzer"""
        self.data_dir = data_dir
        self.data = {}
        
    def load_latest_data(self):
        """Load the most recent data files"""
        print("Loading data files...")
        
        data_types = ['amenities', 'buildings', 'roads', 'landuse', 'transport', 'population']
        
        for data_type in data_types:
            # Find most recent file
            files = [f for f in os.listdir(self.data_dir) 
                    if f.startswith(f'tunisia_{data_type}_') and f.endswith('.csv')]
            
            if files:
                latest_file = sorted(files)[-1]
                filepath = os.path.join(self.data_dir, latest_file)
                
                try:
                    df = pd.read_csv(filepath)
                    self.data[data_type] = df
                    print(f"  ✓ Loaded {data_type}: {len(df)} records")
                except Exception as e:
                    print(f"  ✗ Error loading {data_type}: {e}")
        
        print(f"\nTotal datasets loaded: {len(self.data)}")
    
    def analyze_amenities(self) -> Dict:
        """Analyze amenities data"""
        if 'amenities' not in self.data:
            return {}
        
        df = self.data['amenities']
        
        analysis = {
            'total': len(df),
            'by_city': df['city'].value_counts().to_dict(),
            'by_type': df['amenity_type'].value_counts().to_dict(),
            'with_name': df['name'].notna().sum(),
            'with_phone': df['phone'].notna().sum(),
            'with_website': df['website'].notna().sum(),
        }
        
        return analysis
    
    def analyze_urbanization(self) -> Dict:
        """Analyze urbanization metrics"""
        analysis = {}
        
        # Buildings analysis
        if 'buildings' in self.data:
            df = self.data['buildings']
            analysis['buildings'] = {
                'total': len(df),
                'by_city': df['city'].value_counts().to_dict(),
                'by_type': df['building_type'].value_counts().to_dict(),
            }
        
        # Land use analysis
        if 'landuse' in self.data:
            df = self.data['landuse']
            analysis['landuse'] = {
                'total': len(df),
                'by_type': df['landuse_type'].value_counts().to_dict(),
            }
        
        # Roads analysis
        if 'roads' in self.data:
            df = self.data['roads']
            analysis['roads'] = {
                'total': len(df),
                'by_type': df['highway_type'].value_counts().to_dict(),
            }
        
        return analysis
    
    def calculate_urban_scores(self) -> pd.DataFrame:
        """Calculate urbanization scores for each city"""
        scores = []
        
        # Get unique cities
        cities = set()
        for df in self.data.values():
            if 'city' in df.columns:
                cities.update(df['city'].unique())
        
        for city in cities:
            score = {'city': city}
            
            # Count amenities
            if 'amenities' in self.data:
                score['amenities_count'] = len(self.data['amenities'][
                    self.data['amenities']['city'] == city
                ])
            
            # Count buildings
            if 'buildings' in self.data:
                score['buildings_count'] = len(self.data['buildings'][
                    self.data['buildings']['city'] == city
                ])
            
            # Count roads
            if 'roads' in self.data:
                score['roads_count'] = len(self.data['roads'][
                    self.data['roads']['city'] == city
                ])
            
            # Count transport
            if 'transport' in self.data:
                score['transport_count'] = len(self.data['transport'][
                    self.data['transport']['city'] == city
                ])
            
            # Population
            if 'population' in self.data:
                pop_data = self.data['population'][
                    self.data['population']['city'] == city
                ]
                if not pop_data.empty:
                    score['population'] = pop_data.iloc[0]['population']
            
            # Calculate composite score (normalized)
            total_score = 0
            count = 0
            
            for key in ['amenities_count', 'buildings_count', 'roads_count', 'transport_count']:
                if key in score:
                    total_score += score[key]
                    count += 1
            
            score['urbanization_score'] = total_score / count if count > 0 else 0
            
            scores.append(score)
        
        df = pd.DataFrame(scores)
        
        # Normalize score to 0-100
        if 'urbanization_score' in df.columns and len(df) > 0:
            max_score = df['urbanization_score'].max()
            if max_score > 0:
                df['urbanization_score'] = (df['urbanization_score'] / max_score * 100).round(1)
        
        return df.sort_values('urbanization_score', ascending=False)
    
    def generate_comprehensive_report(self) -> str:
        """Generate comprehensive analysis report"""
        report = []
        report.append("╔" + "═"*60 + "╗")
        report.append("║  TUNISIA URBANIZATION & AMENITIES ANALYSIS REPORT       ║")
        report.append("╚" + "═"*60 + "╝")
        report.append(f"\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        # 1. Overall statistics
        report.append("="*60)
        report.append("1. OVERALL STATISTICS")
        report.append("="*60)
        
        for data_type, df in self.data.items():
            report.append(f"\n{data_type.upper()}:")
            report.append(f"  Total records: {len(df):,}")
            
            if 'city' in df.columns:
                report.append(f"  Cities covered: {df['city'].nunique()}")
        
        # 2. Amenities analysis
        if 'amenities' in self.data:
            report.append("\n" + "="*60)
            report.append("2. AMENITIES ANALYSIS")
            report.append("="*60)
            
            amenities_analysis = self.analyze_amenities()
            
            report.append(f"\nTotal amenities: {amenities_analysis['total']:,}")
            report.append(f"With names: {amenities_analysis['with_name']:,}")
            report.append(f"With phone numbers: {amenities_analysis['with_phone']:,}")
            report.append(f"With websites: {amenities_analysis['with_website']:,}")
            
            report.append("\nTop 10 Amenity Types:")
            for amenity, count in list(amenities_analysis['by_type'].items())[:10]:
                report.append(f"  {amenity}: {count:,}")
            
            report.append("\nAmenities by City:")
            for city, count in list(amenities_analysis['by_city'].items())[:10]:
                report.append(f"  {city}: {count:,}")
        
        # 3. Urbanization analysis
        report.append("\n" + "="*60)
        report.append("3. URBANIZATION ANALYSIS")
        report.append("="*60)
        
        urban_analysis = self.analyze_urbanization()
        
        if 'buildings' in urban_analysis:
            report.append(f"\nBuildings: {urban_analysis['buildings']['total']:,}")
            report.append("  Top building types:")
            for btype, count in list(urban_analysis['buildings']['by_type'].items())[:5]:
                report.append(f"    {btype}: {count:,}")
        
        if 'roads' in urban_analysis:
            report.append(f"\nRoad segments: {urban_analysis['roads']['total']:,}")
            report.append("  Road types:")
            for rtype, count in list(urban_analysis['roads']['by_type'].items())[:5]:
                report.append(f"    {rtype}: {count:,}")
        
        if 'landuse' in urban_analysis:
            report.append(f"\nLand use areas: {urban_analysis['landuse']['total']:,}")
            report.append("  Land use types:")
            for ltype, count in list(urban_analysis['landuse']['by_type'].items())[:5]:
                report.append(f"    {ltype}: {count:,}")
        
        # 4. Urban scores
        report.append("\n" + "="*60)
        report.append("4. URBANIZATION SCORES")
        report.append("="*60)
        
        scores_df = self.calculate_urban_scores()
        
        report.append("\nTop 10 Most Urbanized Cities:")
        for idx, row in scores_df.head(10).iterrows():
            city = row['city']
            score = row.get('urbanization_score', 0)
            pop = row.get('population', 'N/A')
            amenities = row.get('amenities_count', 0)
            
            report.append(f"\n  {city}:")
            report.append(f"    Urbanization Score: {score:.1f}/100")
            if pop != 'N/A':
                report.append(f"    Population: {pop:,}")
            report.append(f"    Amenities: {amenities:,}")
        
        # 5. Data quality
        report.append("\n" + "="*60)
        report.append("5. DATA QUALITY METRICS")
        report.append("="*60)
        
        for data_type, df in self.data.items():
            report.append(f"\n{data_type.upper()}:")
            
            # Calculate completeness
            total_fields = len(df.columns)
            non_null_counts = df.count()
            completeness = (non_null_counts.sum() / (len(df) * total_fields) * 100)
            
            report.append(f"  Data completeness: {completeness:.1f}%")
            
            # Show fields with low completeness
            low_completeness = non_null_counts[non_null_counts < len(df) * 0.5]
            if len(low_completeness) > 0:
                report.append(f"  Fields with <50% data:")
                for field, count in low_completeness.items():
                    pct = (count / len(df) * 100)
                    report.append(f"    {field}: {pct:.1f}%")
        
        # 6. Recommendations
        report.append("\n" + "="*60)
        report.append("6. RECOMMENDATIONS")
        report.append("="*60)
        
        report.append("\nBased on the collected data:")
        
        # Check data gaps
        if 'amenities' in self.data:
            df = self.data['amenities']
            no_phone = len(df) - df['phone'].notna().sum()
            if no_phone > len(df) * 0.7:
                report.append(f"\n  ⚠️  {(no_phone/len(df)*100):.1f}% of amenities lack phone numbers")
                report.append("     Consider: Ground surveys or crowdsourcing for contact info")
        
        if 'buildings' in self.data:
            df = self.data['buildings']
            no_levels = len(df) - df['levels'].notna().sum()
            if no_levels > len(df) * 0.8:
                report.append(f"\n  ⚠️  {(no_levels/len(df)*100):.1f}% of buildings lack height data")
                report.append("     Consider: Satellite imagery analysis for building heights")
        
        # Check coverage
        cities_with_data = set()
        for df in self.data.values():
            if 'city' in df.columns:
                cities_with_data.update(df['city'].unique())
        
        if len(cities_with_data) < 10:
            report.append(f"\n  ⚠️  Only {len(cities_with_data)} cities have data")
            report.append("     Consider: Expanding collection to all 24 governorates")
        
        report.append("\n" + "="*60)
        report.append("END OF REPORT")
        report.append("="*60)
        
        return '\n'.join(report)
    
    def export_for_gis(self, output_dir: str = "tunisia_satellite_data"):
        """Export data in GIS-friendly formats"""
        print("\nExporting GIS-ready files...")
        
        for data_type, df in self.data.items():
            if 'lat' not in df.columns or 'lon' not in df.columns:
                continue
            
            # Create GeoJSON-like structure
            features = []
            for idx, row in df.iterrows():
                if pd.notna(row['lat']) and pd.notna(row['lon']):
                    feature = {
                        'type': 'Feature',
                        'geometry': {
                            'type': 'Point',
                            'coordinates': [float(row['lon']), float(row['lat'])]
                        },
                        'properties': {k: v for k, v in row.items() 
                                     if k not in ['lat', 'lon'] and pd.notna(v)}
                    }
                    features.append(feature)
            
            geojson = {
                'type': 'FeatureCollection',
                'features': features
            }
            
            output_file = os.path.join(output_dir, f'tunisia_{data_type}_geo.json')
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(geojson, f, indent=2, ensure_ascii=False)
            
            print(f"  ✓ Exported {data_type}: {output_file}")
    
    def save_report(self, report: str, output_dir: str = "tunisia_satellite_data"):
        """Save analysis report"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_file = os.path.join(output_dir, f'analysis_report_{timestamp}.txt')
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report)
        
        print(f"\n✓ Report saved: {report_file}")
        
        return report_file


def main():
    """Main function"""
    print("╔" + "═"*60 + "╗")
    print("║  Tunisia Satellite Data Analyzer                         ║")
    print("╚" + "═"*60 + "╝\n")
    
    analyzer = TunisiaDataAnalyzer()
    
    # Load data
    analyzer.load_latest_data()
    
    if not analyzer.data:
        print("\n❌ No data found!")
        print("   Run tunisia_satellite_scraper.py first to collect data")
        return
    
    # Generate report
    print("\nGenerating analysis report...")
    report = analyzer.generate_comprehensive_report()
    
    # Print report
    print("\n" + report)
    
    # Save report
    analyzer.save_report(report)
    
    # Export GIS files
    analyzer.export_for_gis()
    
    # Calculate and save urban scores
    print("\nCalculating urbanization scores...")
    scores_df = analyzer.calculate_urban_scores()
    
    scores_file = os.path.join(analyzer.data_dir, 'urbanization_scores.csv')
    scores_df.to_csv(scores_file, index=False, encoding='utf-8')
    print(f"✓ Urban scores saved: {scores_file}")
    
    print("\n" + "="*60)
    print("ANALYSIS COMPLETE")
    print("="*60)
    print(f"\nAll files saved to: {analyzer.data_dir}/")


if __name__ == "__main__":
    main()