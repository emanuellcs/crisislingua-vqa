# CrisisLingua-VQA: Visual Question Answering and Logistics Routing for Humanitarian Crises

**Mandatory Attribution:** _This dataset was adapted and optimized using Adaptive Data by Adaption_.

## 🌍 Context

During rapid-onset natural disasters, first responders rely on real-time situational awareness crowdsourced from affected populations. However, mainstream Vision-Language Models (VLMs) often suffer from "competence collapse" in the Global South due to chaotic, code-switched text and hyper-local geographical references.

**CrisisLingua-VQA** provides a robust benchmark dataset and pipeline for perception and reasoning in these complex contexts, focusing on highly vulnerable regions and under-resourced languages.

### Target Languages (ISO 639-3)

- **East Africa:** Swahili (`swa`), Amharic (`amh`)
- **Southeast Asia (Philippines):** Tagalog (`tgl`), Cebuano (`ceb`)
- **South Asia (India):** Marathi (`mar`), Bhojpuri (`bho`)

---

## 🏗️ Technical Architecture

The pipeline is divided into four distinct phases, now fully integrated with production-ready humanitarian and data-crawling APIs.

### Phase 1: Production Data Acquisition

- **Ushahidi v3 API**: Real-time extraction from verified `.api.ushahidi.io` deployment URLs. Implements full pagination, eager category mapping, recursive media ID resolution for multimodal VQA pairing, and Internet Archive CDX fallback for blocked or unavailable live API responses.
- **CLEAR Global (TWB) Integration**:
  - **LUDP (Language Use Data Platform)**: Automated fetching of regional language usage and terminology data.
  - **Glossary Scraping**: Dynamic BeautifulSoup-based scraping of `glossaries.clearglobal.org` for specialized disaster terminology.
- **Common Crawl (CC-NEWS) Streaming**: High-scale discovery of regional news articles via direct WARC (Web ARChive) streaming. Uses heuristic keyword matching on the raw HTML stream to isolate disaster-relevant image-text pairs.

### Phase 2: Adaptation (Powered by the `adaption` SDK)

Transitioned from raw HTTP calls to the official **Adaptive Data Python SDK** for high-performance data reshaping:

- **`client.ingest()`**: Batch upload of raw multimodal signals.
- **`client.adapt()`**: Executes noise filtering, code-switch normalization, and mapping to **FEMA ESF** / **MIRA** frameworks.
- **`client.export()`**: Generates optimized JSONL artifacts for downstream model training.

### Phase 3: Validation

- **PII Scrubbing**: Automated removal of Personally Identifiable Information to ensure data sovereignty.
- **Distribution Audit**: Statistical verification of dataset balance across the "long-tail" of languages.

### Phase 4: Deployment

- Final export to machine-ready `JSONL` formats for submission to Kaggle and Hugging Face.

---

## 🛡️ Network Safety & Ethics

To prevent IP bans and respect the infrastructure of humanitarian organizations, the pipeline implements hyper-conservative network policies:

- **Circuit Breaker Pattern**: The pipeline tracks consecutive failures. If **three consecutive** `429 (Too Many Requests)` or `403 (Forbidden)` responses are received, the script will log a critical error and exit immediately to prevent further triggering of WAFs or IP bans.
- **`Retry-After` Compliance**: All modules explicitly parse the `Retry-After` HTTP header and will pause execution for the exact duration requested by the server.
- **Mandatory Jitter**: Every single network request (pagination, media resolution, or WARC streaming) is preceded by a mandatory random sleep of `3.0 to 8.0` seconds. This maintains an average throughput of ~11 requests per minute per domain.
- **Archive Fallbacks**: Ushahidi archive lookups use the canonical Internet Archive CDX endpoint with longer connect/read timeouts and retry backoff to tolerate slow or overloaded CDX responses.
- **Ethical User-Agent**: The pipeline identifies itself transparently as:
  `CrisisLingua-Research-Bot/1.0 (Uncharted Data Challenge; Research purpose; limit 20req/min)`

---

## 🛠️ Setup & Usage

### 1. Environment Configuration

Create a `.env` file in the root directory:

```bash
# Ushahidi Deployments (Comma-separated URLs)
USHAHIDI_DEPLOYMENT_URLS="https://freddymalawi.api.ushahidi.io,https://tclirp.api.ushahidi.io,https://stl.api.ushahidi.io,https://aklfloodsjan23.api.ushahidi.io,https://kenyacovid19.api.ushahidi.io"

# Adaptive Data Platform
ADAPTION_API_KEY="your_api_key_here"
# Optional; omit to use the SDK default. Do not include /v1 or /api/v1.
ADAPTION_BASE_URL="https://api.adaptionlabs.ai"
```

Ushahidi Cloud map/front-end hosts such as `https://example.ushahidi.io` may return the HTML single-page application at `/api/v3/posts`. Use the API host pattern instead: `https://example.api.ushahidi.io`.

### 2. Installation

```bash
pip install -r requirements.txt
```

### 3. Execution Flow

Run the pipeline components in sequence:

```bash
# Generate heuristic keywords from TWB
python src/acquisition/twb_glossary.py

# Collect multimodal data (Common Crawl & Ushahidi)
python src/acquisition/multimodal_scraper.py
python src/acquisition/ushahidi_api.py

# Reshape data via Adaptive Data Platform
python src/adaptation/adaptive_client.py
```

---

## 📊 Repository Structure

```text
src/
├── acquisition/
│   ├── ushahidi_api.py       # Production V3 API client
│   ├── twb_glossary.py       # LUDP & Web Scraping ingestor
│   └── multimodal_scraper.py # CC-NEWS WARC streamer
├── adaptation/
│   ├── adaptive_client.py    # Official SDK integration
│   ├── filter_noise.py       # Logic for cleaning social chatter
│   └── schema_mapper.py      # FEMA/MIRA framework mapping
├── validation/               # PII scrubbing & audits
└── deployment/               # JSONL export logic
```

## ⚖️ License

- **Codebase:** [Apache License 2.0](LICENSE)
- **Dataset:** Creative Commons Attribution 4.0 International (CC BY 4.0)
