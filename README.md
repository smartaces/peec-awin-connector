<img src="awin_peec_logos.png" alt="Peec AI x Awin" width="40%">

# The Unofficial AI Visibility and Affiliate Performance Cookbook

An unofficial experimental connector to link AI Search Visibility and Affiliate Performance metrics :)

This interactive Jupyter notebook that overlays AI citation visibility data from [Peec AI](https://peec.ai) with affiliate transaction data from [Awin](https://awin.com), helping you understand which publisher domains are both cited by AI models and driving affiliate revenue.

> **Disclaimer**: This is **not** an official connector produced by Peec AI or Awin. It is a community project sharing example code of how affiliate and AI visibility platform data can be combined to yield insights. Use the contents of the repository at your own discretion. The authors accept no liability for any issues arising from its use.

## Run The Cookbook in Google Colab:

For the easiest and quickest setup, run the cookbook in Google Colab: 

https://colab.research.google.com/github/smartaces/peec-awin-connector/blob/main/Peec_AI_x_Awin_Cookbook_Colab.ipynb

Alternatively full setup instructions to run on your local machine are provided below.

## What It Does

- **Pulls AI citation data** from the Peec AI API — which domains and URLs are being cited by AI models (ChatGPT, Gemini, Perplexity, Claude, etc.)
- **Pulls affiliate transaction data** from the Awin API — which publishers are driving sales
- **Matches domains** between the two datasets using normalised hostname matching
- **Produces an enriched report** showing citation metrics alongside transaction revenue for matched publishers
- **Identifies gaps** — domains cited by AI models where you have no Awin publisher relationship (potential recruitment targets)

*Example output — enriched report combining AI citation data with affiliate transaction metrics:*

![Peec AI x Awin Insights Dashboard](peec_awin_insights_dashboard.png)

## Reports

| Report | Description |
|--------|-------------|
| **Domain Report** | One row per domain — total citations, avg position, models present |
| **URL Report** | One row per URL — page-level citation breakdown with filters |
| **Awin Transactions** | Transaction summary by publisher with domain extraction |
| **Enriched Report** | Matched domains with citations + revenue + AI model codes + domain type filter |
| **Gap Analysis** | AI-cited domains NOT in your Awin programme — recruitment targets |

## Prerequisites

- A [Peec AI](https://peec.ai) account and API key
- An [Awin](https://awin.com) advertiser account and API token
- Python 3.8+ with Jupyter support

## Setup

### Google Colab

1. Open `Peec_Awin_Connector.ipynb` in Google Colab
2. Add your API keys as Colab secrets:
   - `PEEC_API_KEY` — your Peec AI API key
   - `AWAPI` — your Awin API token
3. Run cells in order — the bootstrap cell downloads scripts automatically from this repo

### Local (VS Code / Jupyter)

1. Clone this repository:
   ```bash
   git clone https://github.com/smartaces/peec-awin-connector.git
   cd peec-awin-connector
   ```

2. Install dependencies:
   ```bash
   pip install requests pandas python-dotenv ipywidgets
   ```

3. Create a `.env` file in the project root:
   ```
   PEEC_API_KEY=your_peec_api_key_here
   AWAPI=your_awin_api_token_here
   ```

4. Open `Peec_Awin_Connector.ipynb` and run cells in order

## Architecture

The notebook uses a modular cell-based architecture. Each logical step lives in its own Python script, loaded by the notebook via `exec()`:

```
scripts/
├── cell_00_pip_installs.py        # Dependency installation
├── cell_01_session_config.py      # API keys, project, dates, advertiser ID
├── cell_02_css_styling.py         # Shared CSS for report styling
├── cell_03_peec_client.py         # Peec AI API client + helper functions
├── cell_04_peec_data_pull.py      # Pull citation data from Peec AI
├── cell_05_domain_report.py       # Domain-level aggregation
├── cell_06_url_report.py          # URL/page-level aggregation
├── cell_07_awin_transactions.py   # Awin transaction fetch (auto-chunked)
├── cell_09_enriched_report.py     # Domain match + enrichment + filters
└── cell_10_gap_analysis.py        # Unmatched domain identification
```

Scripts share state via `__main__` globals and can be updated independently on GitHub without modifying the notebook.

## API Notes

- **Peec AI**: Uses `X-API-Key` header authentication against `https://api.peec.ai/customer/v1`
- **Awin**: Uses `accessToken` query parameter. The transaction endpoint has a 31-day maximum window per request — the connector handles chunking automatically for longer date ranges

## Connect

Built by [James Bentley](https://www.linkedin.com/in/jamesbentleyai/) — feel free to connect on LinkedIn.

## License

This project is provided as-is for educational and community use. It is not affiliated with, endorsed by, or supported by Peec AI or Awin.
