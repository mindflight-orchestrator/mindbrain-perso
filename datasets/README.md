# Datasets

Bind IMDb and YAGO sources here when running the benchmark compose stack.

Expected layout:

- `datasets/imdb/` containing the standard IMDb TSV files, or `datasets/imdb/imdb-datasets/`
- `datasets/` containing the standard IMDb TSV files directly
- `datasets/yago/` containing extracted YAGO RDF files, or set `YAGO_PATH` to a zip archive

Do not commit the actual datasets into this repository.
