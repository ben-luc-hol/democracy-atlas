> Interactive election maps & graphics, past and present  
> **Initial Launch:** 2025-08-01
---
## 🚀 Project Overview

**electoral-explorer** is an interactive web app for visualizing Norwegian election results from 1972 through live vote-counting. Built under contract for a major Norwegian news organization (announcement to follow).

- **Slide through time** to see how each county/municipality voted in every election since 1973
- **Explore public data** about economic and social conditions around the country and correlations with voting patterns
- **Compare parties** across years and geography  
- **Watch live results** roll in on election night (September 8, 2025)
- **Access exit poll results** as they come in.

## 🎯 Key Features

1. **Historical Data Pipeline**  
   - Scrape & normalize official election results (1972–2025)  
   - Store in a standard Parquet/CSV format  
2. **Live Data Ingestion**  
   - Connect to the vote-count API for real-time updates  
   - Smooth incremental updates during vote counting  
3. **Geographic Mapping**  
   - Use county & municipality shapefiles  
   - Interactive choropleth & cartogram views  
4. **Time Slider UI**  
   - Seamless year-by-year transition  
   - “Play” election-night animation  
5. **Analytics & Charts**  
   - Party performance over time  
   - Turnout, blank ballots, invalid votes, etc.  

## 📂 Repository Structure (In progress)

##  🛠 Architecture & Tech

**Back-end:** Python (FastAPI)

**Front-end:** React + D3 + TailwindCSS

**Data:** Parquet/CSV on S3

**CI/CD:** GitHub Actions → Docker → Kubernetes

**Mapping:** geoJSON, TopoJSON, Shapely/Geopandas

## 📅 Roadmap
   -  Milestone	Target Date
   -  Historical ETL pipeline	2025-05-15
   -  Basic UI & slider	2025-06-01
   -  Live-data integration	2025-07-01
   -  Styling & polish	2025-07-20
   -  Launch date	2025-08-01


📄 License
MIT © Ben Holden 2025
