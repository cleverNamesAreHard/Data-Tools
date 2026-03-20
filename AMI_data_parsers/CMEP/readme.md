## Overview

A CMEP parser (currently only MEPMD01) with a simple goal: take vendor-exported CMEP files, validate them well, then output an ingestion-friendly CSV output.

108 unit tests cover valid input and expected failure modes for MEPMD01 format.

## Status

**Implemented**:
- MEPMD01 (interval data), validated and flattened to one interval per output row

**Planned**:
- Additional CMEP data types will be added as dedicated parsers, plus unit tests, following the same "fail-fast" philosophy.

## Usage

```
python3 parse_mepmd01.py /path/to/cmep_file.csv
```

Optional output path:

```
python3 parse_mepmd01.py -o /path/to/output.csv /path/to/cmep_file.csv
```

Output path rules:
- If `-o` is omitted, output is written alongside the input as `<input_stem>_parsed.csv`.
- If `-o` is provided without a directory component, it is written alongside the input file.
- If `-o` includes a directory, output is written to that path.

## Generating Documentation

```
python generate_docs.py
```

Documentation is generated in HTML and Markdown formats, and are available in `docs/`.  It relies on `docs/spec_table.html`, and should not be deleted, or `generate_docs.py` will fail.

## File Formats

CMEP files are CSV files with no header row.

### MEPMD01 Output Format

For easy parsing, output is contains one interval per row.

Each output row repeats the record fields (columns 1:14) and includes exactly one interval triplet:
- IntervalDateTime (YYYYMMDDHHMM)
- Protocol Text
- Interval Usage

This ensures easy parsing and a symmetrical file, row to row.

## Samples

See `test/*.csv` for sample input files for the various CMEP formats (currently only MEPMD01).
