import os
import time
import http.client
import json
from datetime import datetime, timedelta
import argparse
import re

# API endpoints
API_HOST = "api-metrika.yandex.net"
API_CREATE_REQUEST = "/management/v1/counter/{counter_id}/logrequests"
API_CHECK_REQUEST = "/management/v1/counter/{counter_id}/logrequest/{request_id}"
API_DOWNLOAD_REQUEST = "/management/v1/counter/{counter_id}/logrequest/{request_id}/part/0/download"
API_CLEANUP_REQUEST = "/management/v1/counter/{counter_id}/logrequest/{request_id}/clean"

# Available sources
VISITS = "visits"
HITS = "hits"
AVAILABLE_SOURCES = [VISITS, HITS]

# Visits field definitions, for more fields see:
# https://yandex.ru/dev/metrika/ru/logs/fields/visits
VISITS_FIELDS = [
    # Visit
    "ym:s:visitID",
    "ym:s:dateTime",
    "ym:s:date",
    "ym:s:dateTimeUTC",
    "ym:s:clientTimeZone",


    # User
    "ym:s:clientID",
    "ym:s:counterID",
    "ym:s:isNewUser",
    "ym:s:ipAddress",
    "ym:s:regionCountry",
    "ym:s:regionCity",


    # Device
    "ym:s:deviceCategory",
    "ym:s:browser",


    # UTM
    "ym:s:firstUTMSource",
    "ym:s:lastUTMSource",
    "ym:s:lastsignUTMSource",
    "ym:s:last_yandex_direct_clickUTMSource",

    "ym:s:firstUTMMedium",
    "ym:s:lastUTMMedium",
    "ym:s:lastsignUTMMedium",
    "ym:s:last_yandex_direct_clickUTMMedium",

    "ym:s:firstUTMCampaign",
    "ym:s:lastUTMCampaign",
    "ym:s:lastsignUTMCampaign",
    "ym:s:last_yandex_direct_clickUTMCampaign",

    "ym:s:firstUTMContent",
    "ym:s:lastUTMContent",
    "ym:s:lastsignUTMContent",
    "ym:s:last_yandex_direct_clickUTMContent",

    "ym:s:firstUTMTerm",
    "ym:s:lastUTMTerm",
    "ym:s:lastsignUTMTerm",
    "ym:s:last_yandex_direct_clickUTMTerm",


    # Traffic source
    "ym:s:firstTrafficSource",
    "ym:s:lastTrafficSource",
    "ym:s:lastsignTrafficSource",
    "ym:s:last_yandex_direct_clickTrafficSource",

    "ym:s:firstReferalSource",
    "ym:s:lastReferalSource",
    "ym:s:lastsignReferalSource",
    "ym:s:last_yandex_direct_clickReferalSource",

    "ym:s:firstSearchEngineRoot",
    "ym:s:lastSearchEngineRoot",
    "ym:s:lastsignSearchEngineRoot",
    "ym:s:last_yandex_direct_clickSearchEngineRoot",

    "ym:s:firstSocialNetwork",
    "ym:s:lastSocialNetwork",
    "ym:s:lastsignSocialNetwork",
    "ym:s:last_yandex_direct_clickSocialNetwork",

    "ym:s:firstSocialNetworkProfile",
    "ym:s:lastSocialNetworkProfile",
    "ym:s:lastsignSocialNetworkProfile",
    "ym:s:last_yandex_direct_clickSocialNetworkProfile",

    "ym:s:firstRecommendationSystem",
    "ym:s:lastRecommendationSystem",
    "ym:s:lastsignRecommendationSystem",
    "ym:s:last_yandex_direct_clickRecommendationSystem",

    "ym:s:firstMessenger",
    "ym:s:lastMessenger",
    "ym:s:lastsignMessenger",
    "ym:s:last_yandex_direct_clickMessenger",


    # Visit params
    "ym:s:referer",
    "ym:s:startURL",
    "ym:s:endURL",
    "ym:s:pageViews",
    "ym:s:visitDuration",
    "ym:s:bounce",
    "ym:s:parsedParamsKey1",
    "ym:s:parsedParamsKey2",
    "ym:s:parsedParamsKey3",
    "ym:s:watchIDs",
    "ym:s:goalsID",
    "ym:s:goalsSerialNumber",
    "ym:s:goalsDateTime",
    "ym:s:goalsPrice",
    "ym:s:goalsOrder",
]

# Hits field definitions will be implemented later
# https://yandex.ru/dev/metrika/ru/logs/fields/hits
HITS_FIELDS = []


def compose_yesterday_date_range():
    """Get date range for yesterday and day before yesterday"""
    yesterday = datetime.now() - timedelta(days=1)
    day_before = datetime.now() - timedelta(days=2)
    return (
        day_before.strftime("%Y-%m-%d"),
        yesterday.strftime("%Y-%m-%d")
    )


def create_request(counter_id, auth_token, date1, date2, source=VISITS):
    """Create log request with required fields"""
    if source == HITS:
        raise NotImplementedError("Hits source is not implemented yet")

    headers = {"Authorization": f"OAuth {auth_token}"}
    conn = http.client.HTTPSConnection(API_HOST)
    fields_param = ",".join(VISITS_FIELDS if source == VISITS else HITS_FIELDS)
    url = f"{API_CREATE_REQUEST.format(counter_id=counter_id)}?date1={date1}&date2={
        date2}&source={source}&fields={fields_param}"

    try:
        conn.request("POST", url, headers=headers)
        response = conn.getresponse()

        if response.status != 200:
            error = json.loads(response.read()).get("message", "Unknown error")
            raise RuntimeError(f"API Error {response.status}: {error}")

        return json.loads(response.read())["log_request"]["request_id"]
    finally:
        conn.close()


def wait_for_request(counter_id, auth_token, request_id, timeout_mins=30):
    """Wait until request is processed with timeout"""
    headers = {"Authorization": f"OAuth {auth_token}"}
    conn = http.client.HTTPSConnection(API_HOST)
    attempts = 0

    try:
        print("Waiting for request to be processed...")

        start_time = time.time()
        while time.time() - start_time < timeout_mins * 60:
            attempts += 1
            conn.request(
                "GET",
                API_CHECK_REQUEST.format(
                    counter_id=counter_id, request_id=request_id),
                headers=headers
            )
            response = json.loads(conn.getresponse().read())
            status = response["log_request"]["status"]

            print(f"Attempt {attempts}: status={status}")

            if status == "processed":
                print(f"Request processed after {attempts} attempts.")
                return

            if status in ["canceled", "failed"]:
                raise RuntimeError(f"Request failed: {status}")

            time.sleep(10)

        raise RuntimeError(
            f"Timeout after {timeout_mins} minutes ({attempts} attempts)")
    finally:
        conn.close()


def download_and_save(counter_id, auth_token, request_id, output_dir, date1, date2, source=VISITS):
    """
    Download CSV and save to file

    Args:
        counter_id (str): Yandex Metrika counter ID
        auth_token (str): Yandex Metrika OAuth token
        request_id (str): Request ID to download
        output_dir (str): Directory to save the file
        date1 (str): Start date in YYYY-MM-DD format
        date2 (str): End date in YYYY-MM-DD format
        source (str): Data source type ('visits' or 'hits'). Defaults to 'visits'
    """
    headers = {"Authorization": f"OAuth {auth_token}"}
    conn = http.client.HTTPSConnection(API_HOST)

    try:
        conn.request(
            "GET",
            API_DOWNLOAD_REQUEST.format(
                counter_id=counter_id, request_id=request_id),
            headers=headers
        )
        response = conn.getresponse()

        if response.status != 200:
            raise RuntimeError(f"Download failed: {
                               response.status} {response.reason}")

        content = response.read()
        if len(content) < 1024:
            raise RuntimeError("Downloaded file appears to be empty")

        try:
            os.makedirs(output_dir, exist_ok=True)
        except OSError as e:
            raise OSError(f"Failed to create directory {output_dir}: {e}")

        filename = f"{source}_{date1}_to_{date2}.csv"
        filepath = os.path.join(output_dir, filename)

        with open(filepath, "wb") as f:
            f.write(content)

        return filepath
    finally:
        conn.close()


def cleanup_request(counter_id, auth_token, request_id):
    """Clean up API resources"""
    headers = {"Authorization": f"OAuth {auth_token}"}
    conn = http.client.HTTPSConnection(API_HOST)

    try:
        conn.request(
            "POST",
            API_CLEANUP_REQUEST.format(
                counter_id=counter_id, request_id=request_id),
            headers=headers
        )
        response = conn.getresponse()

        if response.status != 200:
            print(f"Warning: Cleanup failed for request {request_id}")
    finally:
        conn.close()


def validate_date_format(date_str):
    """Validate date string format (YYYY-MM-DD)"""
    pattern = r'^\d{4}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12]\d|3[01])$'
    if not re.match(pattern, date_str):
        raise argparse.ArgumentTypeError(
            f'Invalid date format: {
                date_str}. Please use YYYY-MM-DD format (e.g., 2024-03-21)'
        )
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return date_str
    except ValueError as e:
        raise argparse.ArgumentTypeError(f'Invalid date: {str(e)}')


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Download Yandex Metrika logs for date range')
    parser.add_argument('counter_id', help='Yandex Metrika counter ID')
    parser.add_argument('auth_token', help='Yandex Metrika OAuth token')
    parser.add_argument('--output-dir', default="data",
                        help='Output directory (default: data)')
    parser.add_argument('--source', choices=AVAILABLE_SOURCES, default=VISITS,
                        help=f'Data source type (default: {VISITS})')
    parser.add_argument('start_date', nargs='?', type=validate_date_format,
                        help='Start date in YYYY-MM-DD format')
    parser.add_argument('end_date', nargs='?', type=validate_date_format,
                        help='End date in YYYY-MM-DD format')
    args = parser.parse_args()

    # If one date is provided, both must be provided
    if bool(args.start_date) != bool(args.end_date):
        parser.error('Both start_date and end_date must be provided together')

    # Validate date range if provided
    if args.start_date and args.end_date:
        start = datetime.strptime(args.start_date, '%Y-%m-%d')
        end = datetime.strptime(args.end_date, '%Y-%m-%d')
        if end < start:
            parser.error('End date must be after or equal to start date')
        return args.counter_id, args.auth_token, args.start_date, args.end_date, args.output_dir, args.source

    # Default to yesterday's range if no dates provided
    yesterday = datetime.now() - timedelta(days=1)
    day_before = datetime.now() - timedelta(days=2)
    return (
        args.counter_id,
        args.auth_token,
        day_before.strftime("%Y-%m-%d"),
        yesterday.strftime("%Y-%m-%d"),
        args.output_dir,
        args.source
    )


def download_logs(counter_id, auth_token, date1, date2, output_dir="data", source=VISITS):
    """
    Download Yandex Metrika logs for specified date range.

    Args:
        counter_id (str): Yandex Metrika counter ID
        auth_token (str): Yandex Metrika OAuth token
        date1 (str): Start date in YYYY-MM-DD format
        date2 (str): End date in YYYY-MM-DD format
        output_dir (str, optional): Directory to save the downloaded file. Defaults to "data"
        source (str): Data source type ('visits' or 'hits'). Defaults to 'visits'

    Returns:
        str: Path to the downloaded file

    Raises:
        RuntimeError: If API request fails or times out
        ValueError: If required parameters are empty or invalid
        OSError: If directory creation fails
        NotImplementedError: If hits source is requested (not implemented yet)
    """
    if not counter_id or not auth_token:
        raise ValueError("counter_id and auth_token are required")

    if source not in AVAILABLE_SOURCES:
        raise ValueError(f"Invalid source. Must be one of: {
                         ', '.join(AVAILABLE_SOURCES)}")

    # Create request
    request_id = create_request(counter_id, auth_token, date1, date2, source)
    print(f"Created request ID: {request_id}")

    # Wait for processing
    print("Processing request...")
    wait_for_request(counter_id, auth_token, request_id)

    # Download and save
    print("Downloading data...")
    filepath = download_and_save(
        counter_id, auth_token, request_id, output_dir, date1, date2, source)
    print(f"Successfully saved to: {filepath}")

    # Cleanup
    cleanup_request(counter_id, auth_token, request_id)

    return filepath


def main():
    try:
        counter_id, auth_token, date1, date2, output_dir, source = parse_args()
        print(f"Processing {source} data from {date1} to {date2}")

        filepath = download_logs(
            counter_id, auth_token, date1, date2, output_dir, source)

    except NotImplementedError as e:
        print(f"\nERROR: {str(e)}")
        print("Only 'visits' source is currently supported.")
        exit(1)
    except Exception as e:
        print(f"\nERROR: {str(e)}")
        exit(1)


if __name__ == "__main__":
    start_time = time.time()
    main()
    print(f"\nCompleted in {time.time() - start_time:.1f} seconds")
