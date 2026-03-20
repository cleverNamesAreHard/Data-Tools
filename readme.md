## Overview

This repository is a collection of data tools for parsing, validating, and reshaping structured data files.

It exists to house small, focused utilities that can be reused across projects, rather than rewriting one-off scripts for recurring data tasks.

Some tools are general-purpose, while others are specific to particular data domains, protocols, or vendor formats.

## Current Contents

### `find_asymmetrical_rows/`
A utility for identifying malformed CSV rows whose column counts do not match the header row.

### `AMI/`
Automated Metering Infrastructure (AMI)-related tools for working with various formats.

#### `AMI/CMEP/` California Meter Exchange Protocol (CMEP) files.
Tools for parsing data files in California Meter Exchange Protocol (CMEP) formats.

Currently includes:
- a parser for `MEPMD01` interval data

More CMEP parsers are planned.