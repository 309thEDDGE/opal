# Acceptance Setup Scripts

Scripts designed to parse, translate, and catalog all of the Chapter 10 data in the acceptance environment. 

Can also be repurposed for other environments with a bit of work.

## Chapter 10 Catalog

Given a directory of chapter 10 files, makes a table of:
- The path
- The tip hash (first 150MB)
- The file size

Uses cached results from the previous run if available: if a chapter 10 of the same size exists in the same location, it is assumed to have the same hash.

## Tip Parse Flow

Parses a chapter 10 file, uploads the result to S3 via metaflow.

Tip metadata is included in the metaflow object and uploaded to the catalog.

## Tip Parse Flow Wrapper

Parses all the unparsed chapter 10s in the most recent chapter 10 catalog. Compares the hashes in the chapter 10 catalog with the hashes of the tip parse flow results to avoid duplicates.

## Tip Translate Flow

Translates a parsed chapter 10 dataset (ARINC429 *OR* MILSTD1553) with a DTS file, uploads the result to S3 via metaflow.

Tip metadata is included in the metaflow object and uploaded to the catalog.

## Tip Translate Flow Wrapper

Translates all untranslated parsed datasets of one type (ARINC429 *OR* MILSTD1553) with a DTS file. Checks the tip parse run ids against the inputs to all existing tip translated datasets to avoid duplicates.

## Tip Midnight Catalog

Generates a table of the following fields for each chapter 10:
- chapter 10 name
- chapter 10 path
- S3 path to all parsed datasets
- S3 path to all translated datasets
- Tip parsed flow run id
- Tip translated flow run id

## Regenerate Catalog

Deletes everything in the catalog, and reloads all the data from the given flow names.

--all: publish all valid runs (successful and has data) for these flows
--latest: publish only the latest successful run for this flow

## Tip Utils

Provides command line utilities:
- delete-parsed-duplicates: deletes all tip parsed runs with duplicate chapter 10 hashes that are not the latest run for that hash
- delete-translated-orphans: deletes all tip translated runs which came from a tip parsed run that no longer exists

Also defines some utility functions for working with chapter 10 data, tip parsed data, or tip translated data. 
