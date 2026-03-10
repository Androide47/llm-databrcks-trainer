## llm-databrcks-trainer

This repository contains a small end‑to‑end pipeline for **training and experimenting with LLMs on Databricks** using your own data.
It is intended for teams that want to build **business‑specific, personalized LLMs** (for example, for question answering, knowledge assistants, or internal copilots) on top of their existing documents and datasets.

### What this project does

- **Ingestion**: `src/01_ingestion.py` loads raw data (such as files or tables) into a format that can be processed in Databricks.
- **Cleaning & normalization**: `src/02_cleaning.py` applies basic data cleaning steps (filtering, normalization, simple quality checks) so that the text is ready for downstream LLM tasks.
- **Chunking**: `src/03_chunking.py` splits long documents into smaller, semantically meaningful chunks that work better with LLM context windows and retrieval workflows.
- **Databricks integration**: Configuration in `databricks.yml` and the notebook `llm_curation_credentials.dbquery.ipynb` show how to connect to Databricks, query data, and run the pipeline there.

### Typical use case

1. **Prepare your data** in Databricks (e.g. load PDFs, logs, knowledge base articles, or tables).
2. **Run the ingestion, cleaning, and chunking scripts** to transform that data into high‑quality text chunks.
3. **Use the processed data** to train or fine‑tune an LLM, or to power a retrieval‑augmented generation (RAG) application tailored to your business.

### Project goals

- **Reusable**: Provide a simple, opinionated starting point that you can extend with your own data sources and models.
- **Databricks‑friendly**: Use patterns and configuration that fit naturally into the Databricks ecosystem.
- **LLM‑ready data**: Focus on producing clean, well‑structured chunks of text that are easy to plug into training or RAG pipelines.