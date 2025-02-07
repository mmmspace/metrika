# Yandex Metrika Logs API Downloader

A Python script for downloading raw logs data from Yandex Metrika using their Logs API. Currently supports visits data, with hits support planned for future releases.

> This project was created as part of the [Hustle Every Month](http://hustleeverymonth.ru) initiative. Follow the development journey and other side projects on [Boosty](https://boosty.to/hustleeverymonth).

## Features

- Download visits data for specified date ranges
- Automatic handling of API request lifecycle (create, wait, download, cleanup)
- Progress monitoring with status updates
- Configurable output directory
- Support for both command line usage and programmatic integration

## Prerequisites

1. Yandex Metrika counter ([create one](https://metrika.yandex.ru/))
2. OAuth token with access to Logs API ([get token](https://yandex.ru/dev/metrika/ru/intro/authorization))

## Installation

```bash
git clone https://github.com/mmmspace/metrika.git
cd metrika
```

## Usage

### Command Line

```bash
# Download yesterday's visits data
python yandex_metrika_logs_api.py YOUR_COUNTER_ID YOUR_AUTH_TOKEN

# Download data for specific dates
python yandex_metrika_logs_api.py YOUR_COUNTER_ID YOUR_AUTH_TOKEN 2024-03-01 2024-03-02

# Specify custom output directory
python yandex_metrika_logs_api.py YOUR_COUNTER_ID YOUR_AUTH_TOKEN --output-dir custom_data

# Try hits data (not implemented yet)
python yandex_metrika_logs_api.py YOUR_COUNTER_ID YOUR_AUTH_TOKEN --source hits
```

### Python Code

```python
from yandex_metrika_logs_api import download_logs

filepath = download_logs(
    counter_id="YOUR_COUNTER_ID",
    auth_token="YOUR_AUTH_TOKEN",
    date1="2024-03-01",
    date2="2024-03-02",
    output_dir="data",  # optional, defaults to "data"
    source="visits"     # optional, defaults to "visits"
)
print(f"Downloaded file: {filepath}")
```

## Output

Files are saved with descriptive names including the source and date range:

```
data/visits_2024-03-01_to_2024-03-02.csv
```

## Documentation Links

- [Logs API Overview](https://yandex.ru/dev/metrika/ru/logs/)
- [Authorization Guide](https://yandex.ru/dev/metrika/ru/intro/authorization)
- [Quick Start Guide](https://yandex.ru/dev/metrika/ru/logs/practice/quick-start)
- [Visits Fields Reference](https://yandex.ru/dev/metrika/ru/logs/fields/visits)
- [Hits Fields Reference](https://yandex.ru/dev/metrika/ru/logs/fields/hits)

## Getting OAuth Token

1. Create an app in [Yandex OAuth](https://oauth.yandex.ru/)
2. Add required Metrika permissions
3. Generate OAuth token
4. Use token in requests as shown in examples

## License

MIT
