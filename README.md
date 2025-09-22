**Kenya Population and Public Facility Analysis**

This project analyzes the distribution of schools and health facilities across Kenyan counties and sub-counties, assesses infrastructure per capita, and provides insights for prioritizing investments in public services.

**Table of Contents**

Overview

Objectives

Project Structure

Data Sources

Installation

Usage

Key Results

Recommendations

Contributing

License

**Overview**

This analysis aims to understand how population relates to the distribution of schools and health facilities in Kenya. By combining population data with geospatial facility data, the project:

Computes per-capita metrics for health and education facilities.

Visualizes disparities across counties.

Predicts facility needs based on population.

Provides evidence-based recommendations for investment prioritization.

The workflow follows an Access → Assess → Address approach:

Access: Data acquisition and preprocessing.

Assess: Analysis, visualization, and statistical modeling.

Address: Recommendations and actionable insights.

**Objectives**

Evaluate the relationship between population and public service facilities.

Identify counties with the greatest gaps in schools and health facilities per capita.

Provide actionable recommendations for resource allocation.

**Project** **Structure**
miniproject/
│
├─ data/ # CSV and shapefiles
├─ fynesse/
│ ├─ access.py # Data loading and preprocessing
│ ├─ assess.py # Analysis and visualization functions
│ └─ address.py # Recommendations and per-capita metrics
├─ notebooks/
│ └─ Kenya_Facility_Analysis.ipynb
├─ README.md
└─ requirements.txt # Python dependencies

**Data** **Sources**

Population Data: Kenya National Bureau of Statistics – County and Sub-county populations.

Health Facilities: HOTOSM Kenya Health Facility Points Shapefile.

Schools: HOTOSM Kenya Education Facility Points Shapefile.

All geospatial datasets are publicly available via GitHub or HOTOSM. CSV population data is stored in the data/ folder for reproducibility.

**Installation**

Clone the repository and install required packages:

git clone https://github.com/zippyzippy0/miniproject.git
cd miniproject
pip install -r requirements.txt

**Dependencies** **include**:
pandas, geopandas, numpy, seaborn, matplotlib, scikit-learn, osmnx

Note: osmnx may require extra system dependencies for geospatial operations.

**Usage**

Open the main notebook:

jupyter notebook notebooks/Kenya_Facility_Analysis.ipynb

All data is loaded using reproducible paths, either from the local data/ folder or directly from GitHub.

Key functions:

access.load_local_csv() – Loads CSVs from local or GitHub.

access.load_local_shapefile() – Loads shapefiles from local folder or zipped GitHub URL.

assess.plot_per_capita() – Visualizes per-capita facilities.

address.compute_distances() – Calculates distances from county centroids to nearest facilities.

**Key** **Results**

Correlation Analysis: Strong positive correlation between population and number of schools (R² ≈ 0.55) and health facilities (R² ≈ 0.56).

Priority Counties: Counties with lowest hospitals per 100k population: Kericho, Vihiga, Bungoma, Mandera, Tharaka-Nithi.

Schools per 10k population: Mandera, Kericho, Elgeyo-Marakwet, Nyamira, Kisii.

Predictive Modeling: Linear regression predicts facility needs based on population to guide resource allocation.

Visualizations highlight disparities in facility distribution across counties and sub-counties.

**Recommendations**

Focus investments in counties with the greatest per-capita deficits.

Expand health and educational facilities proportionally to population growth.

Consider geospatial accessibility when planning new infrastructure.

Use predictive models to anticipate future service needs in rapidly growing regions.

**Contributing**

Contributions are welcome via pull requests. Please follow PEP8 style and include clear documentation for any new analysis functions.

**License**
MIT License – see LICENSE file for details.
