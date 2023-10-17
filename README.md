# mapper-safegraph

## Overview

The [safegraph_mapper.py](safegraph_mapper.py) python script converts the SafeGraph Places data to json files ready to load into Senzing.

You can purchase SafeGraph Places data at [www.safegraph.com/pricing](https://www.safegraph.com/pricing).  They have even provided free samples
of their data already mapped to Senzing json format via this mapper [here](https://www.safegraph.com/free-data/senzing-data-sample)

Usage:

```console
python safegraph_mapper.py --help
usage: safegraph_mapper.py [-h] [-i INPUT_PATH] [-o OUTPUT_FILE] [-l LOG_FILE]

optional arguments:
  -h, --help            show this help message and exit
  -i INPUT_FILE, --input_file INPUT_FILE
                        the name of the input file
  -o OUTPUT_FILE, --output_file OUTPUT_FILE
                        the name of the output file
  -l LOG_FILE, --log_file LOG_FILE
                        optional name of the statistics log file
```

## Contents

1. [Prerequisites](#prerequisites)
2. [Installation](#installation)
3. [Configuring Senzing](#configuring-senzing)
4. [Running the mapper](#running-the-mapper)
5. [Loading into Senzing](#loading-into-senzing)

### Prerequisites

- python 3.6 or higher
- Senzing API version 3.1 or higher

### Installation

Place the the following files on a directory of your choice ...

- [safegraph_mapper.py](safegraph_mapper.py)
- [safegraph_config_updates.g2c](safegraph_config_updates.g2c)


### Configuring Senzing

*Note:* This only needs to be performed one time! In fact you may want to add these configuration updates to a master configuration file for all your data sources.

Loading the SafeGraph Places data into Senzing requires some additional configuration provoded in the [safegraph_config_updates.g2c](safegraph_config_updates.g2c) file.
To apply it, from your Senzing project's python directrory type ...

```console
python3 G2ConfigTool.py <path-to-file>/safegraph_config_updates.g2c
```

### Running the mapper

Safegraph places data comes in a csv file format.   Once you download it, in a terminal session, navigate to where you downloaded this mapper and type ...
```console
python3 safegraph_mapper.py -i /download_path/safegraph_data.csv -o /output_path/safegraph_data.json
```
- Add the -l --log_file argument to generate a mapping statistics file


### Loading into Senzing

If you use the G2Loader program to load your data, from the /opt/senzing/g2/python directory ...

```console
python3 G2Loader.py -f /output_path/safegraph_data.json
```
