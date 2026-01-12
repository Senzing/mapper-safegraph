# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This repository contains a data mapper that converts SafeGraph Places CSV data to JSON format for loading into Senzing entity resolution software. The mapper transforms business location data (addresses, phone numbers, categories, coordinates) into Senzing-compatible entity records.

## Commands

### Install dependencies
```bash
python -m pip install --group all .
```

### Run the mapper
```bash
python src/safegraph-mapper.py -i <input.csv> -o <output.json> [-l <log_file.json>]
```

### Lint
```bash
pylint $(git ls-files '*.py' ':!:docs/source/*')
```

## Architecture

The codebase consists of two files in `src/`:

- **safegraph-mapper.py**: Single-file mapper with a `mapper` class that:
  - `map()`: Transforms a single CSV row to Senzing JSON format, mapping SafeGraph fields to Senzing attributes (e.g., `LOCATION_NAME` → `LOCATION_NAME_ORG`, `LATITUDE` → `BUSINESS_GEO_LATITUDE`)
  - Handles parent-child relationships via `REL_POINTER_*` fields for hierarchical place data
  - Collects statistics for logging via `stat_pack`

- **safegraph-config-updates.g2c**: Senzing configuration commands to add the SAFEGRAPH data source and configure entity resolution features (PLACEKEY identifier, GEO_LOC matching)

## Data Flow

1. CSV input with SafeGraph Places fields (PLACEKEY, LOCATION_NAME, addresses, coordinates, etc.)
2. Mapper cleans values, transforms field names, handles relationships
3. JSON-lines output with one Senzing entity record per line
4. Optional statistics log file with field coverage data

## Senzing-Specific Guidance

Follow Senzing standards at: https://raw.githubusercontent.com/senzing-factory/claude/refs/tags/v1/commands/senzing.md
