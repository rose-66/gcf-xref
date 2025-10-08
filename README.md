Xref GCS → External Tables Processor (GCF Gen2)

### Overview
This Google Cloud Function (Gen2) processes CSV files uploaded to the `xref-landing-zone` bucket. For each file:
- **Loads a dataset-specific config** from `CONFIG_BUCKET` under `config/[file_stem].json`
- **Validates column count** using a quick read of the first row
- **Enforces a filename pattern** from the config
- **Copies valid files** into `xref-ext-tables/[target_path]/ingestion_timestamp=YYYYMMDD_HHMMSS/<original_path>`
- **Routes failures** to the Dead Letter bucket with an error prefix

Entry point: `xref_processor` in `main.py` (CloudEvent triggered by GCS finalize events).

### Flow
```
xref file upload (gsutil) → xref-landing-zone (trigger) → (Eventarc) gcf-xref-processor → (validate/route) { xref-ext-tables (success), xref-dead-letter (fail) }
```

### Repository layout
- `main.py`: Cloud Function implementation
- `test_main.py`: Unit tests using `pytest` and `unittest.mock`
- `deploy.sh`: Example deployment command (gcloud)
- `requirements.txt`: Python dependencies
- `config/*.json`: Per‑dataset rules used at runtime

### Runtime behavior
1. Receives a finalize event for `gs://xref-landing-zone/<path>/<file>.csv`.
2. Determines config name from the file stem (e.g., `raw_2024_tableau_data_fw20.csv` → `config/raw_2024_tableau_data_fw20.json`).
3. Loads config from `gs://$CONFIG_BUCKET/config/<stem>.json` with the following required keys:
   - `expected_columns` (integer)
   - `filename_pattern` (glob string matched against the full blob name)
   - `target_path` (destination prefix under `xref-ext-tables`)
4. Validates filename pattern and column count (reads CSV with `encoding='latin-1'`, `nrows=1`, `header=None`).
5. On success, copies the blob to `gs://xref-ext-tables/<target_path>/ingestion_timestamp=<ts>/<original_path>`.
6. On failure (config missing, pattern mismatch, column mismatch, unexpected error), copies to Dead Letter bucket under `error/<timestamp>_<original_name>`.

Hardcoded buckets in code:
- Source: `xref-landing-zone`
- Destination: `xref-ext-tables`

Environment variables:
- `CONFIG_BUCKET` (required): GCS bucket name containing `config/*.json`
- `DEAD_LETTER_BUCKET` (required): GCS bucket name for dead letters

### Config schema
Each config is stored in `gs://$CONFIG_BUCKET/config/<stem>.json`. Example:

```json
{
  "expected_columns": 9,
  "filename_pattern": "raw_2024_tableau_data_fw20.csv",
  "target_path": "fixed_vs_adg_orders/"
}
```

Notes:
- `filename_pattern` is matched against the full object path (e.g., `folder/file.csv`) using `fnmatch`. Use wildcards as needed, e.g. `folder/*.csv`.
- `target_path` can end with or without a trailing slash; it will be normalized.

### Local testing
Requirements:
- Python 3.11
- `pip install -r requirements.txt`
- `pip install pytest`

Run tests:
```bash
cd gcf
pytest -q
```

The tests mock GCS I/O and environment variables. No real cloud resources are used.

### Deploy (Gen2)
Use the provided script as a reference. Update project, region, and service account as appropriate.

```bash
gcloud functions deploy gcf-xref-processor \
  --gen2 \
  --runtime python311 \
  --entry-point xref_processor \
  --region us-central1 \
  --source . \
  --trigger-event google.cloud.storage.object.v1.finalized \
  --trigger-resource xref-landing-zone \
  --set-env-vars CONFIG_BUCKET=xref-config,DEAD_LETTER_BUCKET=xref-dead-letter \
  --service-account <your-service-account>@<your-project>.iam.gserviceaccount.com \
  --project <your-project> \
  --memory 512Mi
```

Or run the included `deploy.sh` after editing its values:

```bash
cd gcf
bash deploy.sh
```

### Permissions
The Cloud Function service account must have at minimum:
- `roles/storage.objectViewer`
- `roles/storage.objectCreator` 
- `roles/run.invoker`
- `roles/eventarc.eventReceiver`

### Observability
- Logs are emitted via Python `logging` and viewable in Cloud Logging.
- Success path logs the destination URI and column count.
- Dead letter path logs the reason and target URI.

### Operational notes
- Files are downloaded to `/tmp/<filename>` during validation and then cleaned up.
- CSVs are read using `encoding='latin-1'`. Adjust in code if your datasets require a different encoding.
- Only the file stem is used to locate the config; ensure a config exists for every incoming dataset name.
- If you organize incoming files under subfolders, ensure `filename_pattern` accounts for the full blob path.

### Example configs in this repo
See `config/` for samples like:
- `raw_2024_tableau_data_fw20.json`
- `raw_npi_to_organization.json`

These demonstrate the required shape and how to set `target_path`.

### dbt project (xref_tables)

The `xref_tables/` directory is a dbt project that models the external data into queryable tables.

- `models/sources/*.yml`: Source definitions (table and column docs)
- `models/raw_tables/*.sql`: External table creation from the ext-tables bucket
- `models/stg_tables/*.sql`: Staging models that select from external tables and add an ingestion timestamp column
- `models/tables/*.sql` (optional): Curated downstream models

dbt basics:

```bash
cd xref_tables
dbt run --select ext_tables   # create/refresh external tables
dbt run --select stg_tables   # build staging tables that include timestamps
dbt test                      # run tests
```

Configure your local dbt profile (`profiles.yml`) to point at the correct BigQuery project/dataset. The models reference `{{ target.project }}` and a dataset like `slv_xref`.

### Manual trigger test (ad hoc)
To simulate a finalize event locally (for debugging only), you can call `xref_processor` with a mock CloudEvent in a REPL. Typically the unit tests already cover these flows.

### Requirements
Minimal set in `requirements.txt`:
- `functions-framework`
- `google-cloud-storage`
- `pandas`

### Troubleshooting
- Missing config file → File is routed to Dead Letter; verify `CONFIG_BUCKET` and the presence of `config/<stem>.json`.
- Pattern mismatch → Confirm `filename_pattern` matches the full object path.
- Column mismatch → Ensure `expected_columns` equals the CSV column count.
- Permission denied → Verify service account IAM permissions.

