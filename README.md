# Airtable Base Analyzer

Generate production-grade duplication guides for Airtable bases using Gemini 2.5 Pro. The analyzer downloads a base schema via the Airtable Metadata API, normalizes the structure with strict Pydantic models, prompts Gemini for an expert walkthrough, and emits a richly formatted markdown report.

## Key Features
- Fully typed, object-oriented design with clear separation of concerns.
- Airtable Metadata API integration with rate limiting, exponential backoff, and pagination support.
- Strict data validation using Pydantic for API responses, internal models, and LLM outputs.
- Gemini 2.5 Pro/Flash integration with structured JSON prompting.
- Comprehensive markdown report covering tables, fields, views, relationships, and duplication steps.
- Typer-powered CLI plus example scripts for live and mock executions.
- Extensive pytest suite with mocks and end-to-end coverage.

## Architecture Overview
| Component | Responsibility |
| --- | --- |
| `AirtableClient` | Fetch schema data with retries, pagination, and validation. |
| `SchemaProcessor` | Normalize metadata, derive relationships, and compute creation order. |
| `GeminiClient` | Generate structured duplication guides using Gemini 2.5. |
| `ReportBuilder` | Assemble a detailed markdown report. |
| `AirtableAnalysisService` | Orchestrate the end-to-end workflow. |
| `cli.py` | Command-line interface for running analyses. |

All data transitions are validated with Pydantic models defined in `models.py`. Logging is centralized via `logging_config.py`.

## Getting Started
1. **Clone the repository** and create a virtual environment.
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # Windows: .venv\Scripts\activate
   ```
2. **Install dependencies.**
   ```bash
   pip install -r requirements.txt
   ```
3. **Configure environment variables.** Copy `.env.example` to `.env` and supply your credentials:
   - `AIRTABLE_ACCESS_TOKEN`
   - `AIRTABLE_BASE_ID`
   - `GEMINI_API_KEY`
   - (optional overrides) `GEMINI_MODEL`, `REQUEST_TIMEOUT_SECONDS`, `MAX_RETRY_ATTEMPTS`, `INITIAL_BACKOFF_SECONDS`

## Usage
### Command Line
```bash
python -m airtable_analyzer.cli analyze --output reports/base.md
```
Flags:
- `--base-id/-b`: override the target base ID.
- `--model/-m`: switch between `gemini-2.5-pro` and `gemini-2.5-flash`.
- `--output/-o`: path to save the markdown report.
- `--verbose/-v`: enable debug logging.

### Python API
```python
from airtable_analyzer.airtable_client import AirtableClient
from airtable_analyzer.config import get_settings
from airtable_analyzer.gemini_client import GeminiClient
from airtable_analyzer.report_builder import ReportBuilder
from airtable_analyzer.schema_processor import SchemaProcessor
from airtable_analyzer.service import AirtableAnalysisService

settings = get_settings()
service = AirtableAnalysisService(
    settings=settings,
    airtable_client=AirtableClient(
        access_token=settings.get_airtable_token(),
        timeout_seconds=settings.request_timeout_seconds,
        max_retries=settings.max_retry_attempts,
        initial_backoff_seconds=settings.initial_backoff_seconds,
    ),
    schema_processor=SchemaProcessor(),
    gemini_client=GeminiClient(
        api_key=settings.get_gemini_api_key(),
        model_name=settings.gemini_model,
    ),
    report_builder=ReportBuilder(),
)
markdown = service.generate_report()
print(markdown)
```

### Examples
- `examples/run_analysis.py`: full workflow against a real base.
- `examples/mock_analysis.py`: complete offline run with mock data (ideal for demos/tests).

## Testing & Quality
- Run tests: `pytest`
- Coverage report: `pytest --cov`
- Static analysis:
  - `flake8`
  - `mypy airtable_analyzer`
  - `black --check .`

Tests stub external calls and include an end-to-end flow to guarantee resilience without network access.

## Project Structure
```
.
├── airtable_analyzer/            # Core library code
├── tests/                        # Pytest suite with fixtures and mocks
├── examples/                     # Live and mock usage examples
├── requirements.txt              # Pinned dependencies
├── .env.example                  # Environment variable template
└── README.md
```

## Next Steps & Extensions
1. Add a report export pipeline for PDF/HTML outputs.
2. Introduce base comparison to highlight schema deltas.
3. Track progress with `tqdm` for long-running analyses.
4. Package the CLI for installation (e.g., via `pipx`).

## Troubleshooting
- **Authentication errors**: confirm the Airtable personal access token has the metadata scope and the Gemini key is valid.
- **Rate limit issues**: the client backs off automatically; consider reducing concurrent runs if limits persist.
- **Missing schema fields**: Airtable omits empty configurations—defaults are handled, but review field definitions when expanding the processor.
