# HyP3-ARIA-Orchestration (aka Jupyter Operations)

Demonstrates how to:

1. read a AOI and its related metadata (WIP: standardized geojson files),
2. enumerate SLC pairs for a [GUNW](https://asf.alaska.edu/data-sets/derived-data-sets/sentinel-1-interferograms/),
3. check the DAAC for existing GUNWs, and
4. submit SLC pairs for [HyP3](https://github.com/ASFHyP3) processing

# Installation

Use `pip` to install:

1. `hyp3-sdk`
2. `s1-enumerator`

Make sure your `.netrc` has the following credentials:

```
machine urs.earthdata.nasa.gov
    login <username>
    password <password>
```

# Usage

## Submitting jobs to Hyp3 API using S1-Enumerator

Use [0_Aleutian_Example.ipynb](0_Aleutian_Example.ipynb) to see how an Aleutian AOI can be used to submit jobs to hyp3.

## Translating KMZs to Geojsons

We also have traditionally gotten `*.kmz` files for the AOIs, but for record keeping, we have converted these into geojsons
with metadata that describe enumeration parameters.

