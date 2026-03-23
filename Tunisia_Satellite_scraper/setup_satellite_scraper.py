"""
Setup Tunisia Satellite Scraper - Creates all files and folders
"""
import os
from pathlib import Path

# File contents
FILES = {
    "requirements.txt": """requests==2.31.0
pandas==2.2.0
beautifulsoup4==4.12.3
matplotlib==3.8.2
seaborn==0.13.1
""",
    
    "QUICKSTART.md": """# Tunisia Satellite Data Scraper - Quick Start

## Install
```bash
pip install -r requirements.txt
```

## Run
```bash
python tunisia_satellite_scraper.py
```

## Analyze
```bash
python tunisia_data_analyzer.py
```

## Examples
```bash
python examples_satellite.py 1
```
""",
    
    "README.md": """# Tunisia Satellite & Geospatial Data Scraper

Collects urbanization and amenities data for Tunisia.

## Data Sources
- OpenStreetMap (OSM)
- Overpass API

## Data Collected
1. Amenities (schools, hospitals, shops)
2. Buildings
3. Roads
4. Land use
5. Public transport
6. Population

## Usage
```bash
python tunisia_satellite_scraper.py
```

## Output
All data saved to `tunisia_satellite_data/` folder.
""",
}

def create_structure():
    # Root folder
    root = Path("tunisia_satellite_scraper")
    root.mkdir(exist_ok=True)
    
    print(f"Creating: {root}/")
    
    # Create files in root
    for filename, content in FILES.items():
        filepath = root / filename
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"  ✓ {filename}")
    
    # Copy main Python files
    source_files = [
        "tunisia_satellite_scraper.py",
        "tunisia_data_analyzer.py", 
        "examples_satellite.py"
    ]
    
    for file in source_files:
        source = Path("/mnt/user-data/outputs") / file
        dest = root / file
        if source.exists():
            dest.write_bytes(source.read_bytes())
            print(f"  ✓ {file}")
    
    # Create data folder
    data_folder = root / "tunisia_satellite_data"
    data_folder.mkdir(exist_ok=True)
    (data_folder / ".gitkeep").touch()
    print(f"  ✓ tunisia_satellite_data/")
    
    print(f"\n✅ Done! Structure created in: {root.absolute()}")
    print("\nNext steps:")
    print(f"  cd {root}")
    print("  pip install -r requirements.txt")
    print("  python tunisia_satellite_scraper.py")

if __name__ == "__main__":
    create_structure()