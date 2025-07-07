import json
import pandas as pd
from typing import Union
import time
from datetime import datetime
import pytz
import os
import requests

from mainsequence.logconf import logger


COINMARKETCAP_API_KEY = os.getenv('COINMARKETCAP_API_KEY')
COINGECKO_API_KEY = os.getenv('COINGECKO_API_KEY')
POLYGON_API_KEY = os.getenv('POLYGON_API_KEY')



def get_with_retry(url, max_retries=3, backoff_factor=10):
        """
        Sends a GET request to the specified URL with retry logic, handling 429 errors appropriately.
        """
        for attempt in range(1, max_retries + 1):
            try:
                response = requests.get(url)
                response.raise_for_status()
                return response  # Return the successful response
            except requests.exceptions.HTTPError as e:
                if response.status_code == 429:
                    # Handle 429 Too Many Requests
                    wait_time = backoff_factor ** attempt
                    logger.warning(f"Received 429 Too Many Requests. Retrying after {wait_time} seconds (exponential backoff)...")
                elif response.status_code == 404:
                    logger.warning(
                        f"Asset not Found")
                    return {}
                else:
                    # For other HTTP errors, decide whether to retry or not
                    if attempt == max_retries:
                        raise  # Re-raise the exception if it's the last attempt
                    else:
                        wait_time = backoff_factor ** attempt
                        logger.warning(f"Attempt {attempt} failed with error: {e}. Retrying in {wait_time} seconds...")
            except requests.exceptions.RequestException as e:
                # Handle non-HTTP errors (e.g., network issues)
                if attempt == max_retries:
                    raise  # Re-raise the exception if it's the last attempt
                else:
                    wait_time = backoff_factor ** attempt
                    logger.warning(f"Attempt {attempt} failed with error: {e}. Retrying in {wait_time} seconds...")
            time.sleep(wait_time)

def get_polygon_financials(ticker, from_date:datetime, to_date:datetime,
                           timeframe:str):
    """
    Fetch financial fundamentals for a given ticker using the Polygon API,
    filtered by a filing date range.

    Parameters:
        ticker (str): The ticker symbol.
        api_key (str): Your Polygon API key.
        from_date (str): The earliest filing date in 'YYYY-MM-DD' format (filing_date.gte).
        to_date (str): The latest filing date in 'YYYY-MM-DD' format (filing_date.lte).

    Returns:
        list or None: The financial data (list of results) from the Polygon API, or None if not found.
    """
    from_str = from_date.strftime("%Y-%m-%d")
    to_str = to_date.strftime("%Y-%m-%d")
    base_url = "https://api.polygon.io/vX/reference/financials"
    # Build the URL with the additional date filters
    url = (
        f"{base_url}"
        f"?ticker={ticker}"
        f"&filing_date.gte={from_str}"
        f"&filing_date.lte={to_str}"
        f"&order=asc&limit=100&sort=filing_date&timeframe={timeframe}"
        f"&apiKey={POLYGON_API_KEY}"


    )

    all_results = []

    while True:
        # Make the request (replace get_with_retry with your actual request method)
        response = get_with_retry(url)
        data = response.json()

        # Extract the page's results
        page_results = data.get("results", [])
        all_results.extend(page_results)

        # Check if there's a next_url to continue pagination
        next_url = data.get("next_url")
        if not next_url:
            # No more pages
            break

        # If the next_url doesn't include an apiKey, append it (often Polygon includes it by default).
        # But let's be safe:
        if "apiKey=" not in next_url:
            connector = "&" if "?" in next_url else "?"
            next_url = f"{next_url}{connector}apiKey={POLYGON_API_KEY}"

        # Update `url` to the next page
        url = next_url

    return all_results




def approximate_market_cap_with_polygon(from_date:datetime, to_date:datetime, symbol:str):
    """
    Approximate historical market cap with shares outstanding and adjusted prices
    Args:
        from_date:
        to_date:
        symbol:

    Returns:

    """



    def get_shares_outstanding(ticker, api_key):
        url = f'https://api.polygon.io/v3/reference/tickers/{ticker}?apiKey={api_key}'

        response = get_with_retry(url)

        data = response.json()
        company_info = data.get('results', {})

        shares_outstanding = company_info.get('share_class_shares_outstanding', company_info.get('weighted_shares_outstanding', None))
        return shares_outstanding

    def get_historical_prices(ticker, api_key, start_date, end_date):
        """
        Fetch historical adjusted close prices for a given ticker and date range.
        """
        url = f'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_date.date()}/{end_date.date()}?adjusted=true&sort=asc&apiKey={api_key}'
        response = get_with_retry(url)

        data = response.json()
        if 'results' in data:
            return pd.DataFrame(data['results'])
        else:
            return pd.DataFrame()

    shares_outstanding = get_shares_outstanding(symbol, POLYGON_API_KEY)

    if shares_outstanding is None:
        return pd.DataFrame()

    historical_data = get_historical_prices(
        symbol,
        POLYGON_API_KEY,
        from_date,
        to_date
    )

    if len(historical_data) == 0 or shares_outstanding is None:
        return pd.DataFrame()

    historical_data['market_cap'] = historical_data['c'] * shares_outstanding
    historical_data["time_index"] = pd.to_datetime(historical_data["t"], unit='ms').dt.tz_localize(pytz.utc).dt.normalize()
    historical_data = historical_data.set_index("time_index").rename(columns={
        "c": "price",
        "v": "volume"
    })
    return historical_data[["market_cap", "price", "volume"]]

def get_stable_coin_symbols_by_name_coinmarketcap(cmc: Union[None,object]):
    from coinmarketcapapi import CoinMarketCapAPI, CoinMarketCapAPIError
    if cmc is None:
        cmc = CoinMarketCapAPI(COINMARKETCAP_API_KEY)

    categories = pd.DataFrame(cmc.cryptocurrency_categories().data)
    stablecoin_details = categories[categories['name'].str.contains("Stablecoin")].iloc[0]
    stable_coins = pd.DataFrame(cmc.cryptocurrency_category(id=stablecoin_details.id).data["coins"])
    stable_coins = stable_coins["symbol"].to_list()

    return stable_coins


def get_latest_market_cap_coingecko(
    symbol: str,
    from_date: datetime,
    to_date: datetime
) -> pd.DataFrame:
    """
    https://docs.coingecko.com/reference/coins-markets
    https://docs.coingecko.com/reference/coins-id-history
    Parameters
    ----------
    exclude_stablecoins
    Returns
    -------

    """
    from pycoingecko import CoinGeckoAPI
    cg = CoinGeckoAPI(api_key=COINGECKO_API_KEY)
    coin_list = cg.get_coins_markets(vs_currency="usd", per_page=250)
    coin_list = pd.DataFrame(coin_list)
    coin_list["symbol"] = coin_list["symbol"].str.upper()

    if symbol not in coin_list["symbol"].to_list():
        logger.warning(f"{symbol} not available in coingecko")
        return pd.DataFrame()

    def get_historical_market_caps(
            from_date: datetime,
            to_date: datetime,
            coin_id: str,
            column_name: str
    ):

        from_timestamp = int(from_date.timestamp())
        to_timestamp =  int(to_date.timestamp())
        try:
            historical_data = cg.get_coin_market_chart_range_by_id(
                id=coin_id,
                vs_currency="usd",
                from_timestamp=from_timestamp,
                to_timestamp=to_timestamp,
                interval="daily",
            )
            # circulating_supply = cg.get_coin_circulating_supply_chart_range(
            #     id=coin_id,
            #     vs_currency="usd",
            #     from_timestamp=from_timestamp,
            #     to_timestamp=to_timestamp,
            #     interval="daily",
            # )


        except Exception as e:
            raise e

        data=pd.concat(
            [pd.DataFrame(v, columns=['timestamp', k]).set_index("timestamp") for k, v in historical_data.items()],
            axis=1)
        data=data.reset_index().rename(columns={"prices":"price","market_caps":"market_cap","total_volumes":"volume"})

        data['time_index'] = pd.to_datetime(data['timestamp'], unit='ms')
        data['time_index'] = data['time_index'].dt.tz_localize(pytz.utc).dt.normalize()
        data=data.set_index("time_index")
        return data[["market_cap","price","volume"]]

    coin = coin_list[coin_list["symbol"] == symbol].iloc[0].to_dict()
    market_caps = get_historical_market_caps(
        from_date=from_date,
        to_date=to_date,
        column_name=coin["symbol"],
        coin_id=coin["id"]
    )

    return market_caps


def get_latest_market_cap_coinmarketcap(exclude_stablecoins: bool)->pd.DataFrame:
    """
    Fetch the latest market cap data from CoinMarketCap API.

    This function retrieves the latest cryptocurrency listings from CoinMarketCap,
    optionally excluding stablecoins. It processes the data to extract relevant
    information such as price, market cap, and fully diluted market cap.

    Args:
        exclude_stablecoins (bool): If True, excludes stablecoins from the results.

    Returns:
        pd.DataFrame: A DataFrame containing processed cryptocurrency data, indexed by last_updated timestamp.
    """

    cmc = CoinMarketCapAPI(COINMARKETCAP_API_KEY)
    r = cmc.cryptocurrency_listings_latest()
    tags_to_remove = []

    if exclude_stablecoins:
        stable_coins=get_stable_coin_symbols_by_name_coinmarketcap(cmc=cmc)
        r.data =[tmp_data for tmp_data in r.data if tmp_data["symbol"] not in  stable_coins ]
        tags_to_remove.extend(['stablecoin','asset-backed-stablecoin'])

    serialize = lambda x: json.dumps(x) if isinstance(x, dict) else x

    filtered_data = []
    for tmp_data in r.data:
        tmp_dict = {}
        for key, value in tmp_data.items():
            if key == "tags":
                continue
            elif key == "quote":
                tags = tmp_data["tags"]
                if not any([t in tags for t in tags_to_remove]):
                    try:
                        tmp_dict["price"] = value["USD"]["price"]
                        tmp_dict["fully_diluted_market_cap"] = value["USD"]["fully_diluted_market_cap"]
                        tmp_dict["market_cap"] = value["USD"]["market_cap"]
                        tmp_dict[key] = serialize(value)
                    except KeyError:
                        tmp_dict[key] = serialize(value)
            else:
                tmp_dict[key] = serialize(value)

        filtered_data.append(tmp_dict)

    filtered_data = pd.DataFrame(filtered_data)
    filtered_data.last_updated = pd.to_datetime(filtered_data["last_updated"])
    filtered_data = filtered_data.set_index("last_updated")
    return filtered_data


