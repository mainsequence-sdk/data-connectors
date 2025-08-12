# Data Connectors Development Guidelines

## Build/Configuration Instructions

### Environment Setup
1. **Python Version**: Requires Python >=3.9.0
2. **Dependency Management**: Uses Pipfile with modular extras for different data sources
3. **Environment Variables**: Copy `.env.example` to `.env` and configure required API keys:
   - `MAINSEQUENCE_TOKEN`: MainSequence platform authentication
   - `TDAG_ENDPOINT`: MainSequence TDAG endpoint (typically http://127.0.0.1:8000 for local)
   - `ALPACA_API_KEY` & `ALPACA_SECRET_KEY`: For Alpaca data connector
   - `POLYGON_API_KEY`: For Polygon data connector
   - `COINGECKO_API_KEY`: For CoinGecko data connector
   - `FRED_API_KEY`: For FRED economic data
   - `VFB_PROJECT_PATH`: Project path for VFB integration

### Installation
```bash
# Install base dependencies
pip install -e .

# Install specific data connector extras
pip install -e .[prices-alpaca]
pip install -e .[prices-binance]

# Or install all dependencies
pip install -r requirements.txt
```
## Project Structure

```
data_connectors/
├── apps/                    # Application modules
├── fundamentals/            # Fundamental data connectors
│   ├── coingecko/          # CoinGecko market cap data
│   ├── databento/          # Databento market data
│   └── polygon/            # Polygon fundamental data
├── prices/                  # Price data connectors
│   ├── alpaca/             # Alpaca equity data
│   ├── binance/            # Binance crypto data
│   ├── databento/          # Databento price data
│   └── valmer/             # Valmer data
├── scripts/                 # Utility scripts
├── websockets/             # WebSocket implementations
├── project_configuration.yaml  # Job scheduling config
└── utils.py                # Common utilities
```

## Development Workflow

1. **Setup**: Configure environment variables and install dependencies
2. **Development**: Create data connector inheriting from DataNode
3. **Testing**: Write tests with mocked external dependencies
4. **Integration**: Add to project_configuration.yaml if needed
5. **Deployment**: Deploy through MainSequence platform

## Debugging Tips

- Use `debug_mode=True` when calling `run()` on data connectors
- Check logs at path specified in `LOGGER_FILE_PATH`
- Use `force_update=True` to bypass caching during development
- Monitor memory usage with `has_sufficient_memory()` for large datasets
- Use `run_in_debug_scheduler()` for testing data node scheduling