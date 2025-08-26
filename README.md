# ERA5 Data Processor

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A comprehensive Python toolkit for downloading, processing, and merging ERA5 reanalysis data from the Climate Data Store (CDS).

## Features

- **Automated Data Download**: Download ERA5 pressure level and surface level data automatically
- **Data Splitting**: Split large NetCDF files into individual time step files using CDO
- **Data Merging**: Merge pressure level, surface level, and precipitation data into unified format
- **Time Range Selection**: Process only specific time ranges of data
- **Cross-Platform Support**: Works on Windows with WSL, Linux, and macOS
- **Comprehensive Logging**: Detailed logging for troubleshooting and monitoring
- **Error Handling**: Robust error handling and recovery mechanisms

## Installation

### Prerequisites

- Python 3.6+
- [CDS API](https://cds.climate.copernicus.eu/api-how-to) account and configuration
- [CDO (Climate Data Operators)](https://code.mpimet.mpg.de/projects/cdo)

### Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/era5-data-processor.git
   cd era5-data-processor
   ```

2. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Configure your CDS API key according to the [official guide](https://cds.climate.copernicus.eu/api-how-to)

4. Install CDO:
   - **Linux/WSL**: `sudo apt-get install cdo`
   - **Conda**: `conda install -c conda-forge cdo`
   - **macOS**: `brew install cdo`

## Usage

### Process Pressure Level Data

```bash
# Process default data (February 2018)
python automated_era5_workflow.py

# Process custom date range
python automated_era5_workflow.py --start-year 2020 --start-month 1 --end-year 2020 --end-month 3
```

### Process Surface Level Data

```bash
# Process February 2018 data
python automated_era5_sl_workflow.py --start-year 2018 --start-month 2 --end-year 2018 --end-month 2

# Process custom date range
python automated_era5_sl_workflow.py --start-year 2020 --start-month 1 --end-year 2020 --end-month 12
```

### Merge Data with Time Range Selection

```bash
# Merge all available data
python conbine.py

# Merge specific time range
python conbine.py --start-time "2018-02-01 00:00" --end-time "2018-02-15 18:00"

# Merge with alternative time format
python conbine.py --start-time "20180201_0000" --end-time "20180228_1800"

# Specify custom paths
python conbine.py --pl-path "./pl" --sl-path "./sl" --tp-path "./tp" --output-path "./data"
```

## Project Structure

```
era5-data-processor/
├── automated_era5_workflow.py      # Pressure level data processor
├── automated_era5_sl_workflow.py   # Surface level data processor
├── conbine.py                     # Data merging tool with time range selection
├── download_era5.py               # Pressure level data downloader
├── era5_sl_downloader.py          # Surface level data downloader
├── split_era5.sh                  # Pressure level data splitting script
├── split_era5_sl.sh               # Surface level data splitting script
├── requirements.txt               # Python dependencies
├── README.md                     # Project documentation
├── LICENSE                       # MIT License
└── .gitignore                    # Git ignore patterns
```

## Output Structure

### Pressure Level Data
```
pl/                                # Split pressure level files
├── era5_20180201_0000_pl.nc
├── era5_20180201_0600_pl.nc
└── ...
```

### Surface Level Data
```
sl/                                # Split surface level files (non-precipitation)
├── era5_20180201_0000_sl.nc
├── era5_20180201_0600_sl.nc
└── ...
tp/                                # Split precipitation files
├── era5_20180201_0000_tp.nc
├── era5_20180201_0600_tp.nc
└── ...
```

### Merged Data
```
data/                              # Merged unified files
├── era5_20180201_0000.nc
├── era5_20180201_0600.nc
└── ...
```

## Configuration

### CDS API Configuration

Ensure your CDS API key is configured in `~/.cdsapirc`:
```
url: https://cds.climate.copernicus.eu/api/v2
key: UID:APIKEY
```

### Customization

You can modify the following parameters in the Python scripts:
- Time ranges
- Pressure levels
- Variables to download
- Time resolution
- Geographic areas

## Troubleshooting

### Common Issues

1. **CDS API Errors**:
   - Verify your API key in `~/.cdsapirc`
   - Check network connectivity
   - Ensure sufficient disk space

2. **WSL Issues**:
   - Install WSL: `wsl --install`
   - Check available distributions: `wsl --list --verbose`
   - Install CDO in WSL: `sudo apt-get install cdo`

3. **CDO Errors**:
   - Verify CDO installation: `cdo --version`
   - Check input file integrity
   - Ensure sufficient memory

### Logging

Detailed logs are saved in the `logs/` directory for troubleshooting.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [Copernicus Climate Change Service](https://climate.copernicus.eu/) for providing ERA5 data
- [Climate Data Operators (CDO)](https://code.mpimet.mpg.de/projects/cdo) for data processing tools
- [xarray](http://xarray.pydata.org/) for NetCDF data handling