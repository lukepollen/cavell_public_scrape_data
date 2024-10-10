cavell_db_insert_script.py - Batch insertion to new database, utilising the JSONL file from OpenAI batch response.

cavell_migration - original script from making calls to the OpenAI API and inserting directly to the database. Note the OpenAI key is redacted.

cavell_dedupe - Dedupes records in the final database

cavell_migration_batch - Sets up the jsonl files we will use to create the batch requests. Steps 1-4 are concerned with creating the JSONL file with the batch requests : Steps 5-7 make the batch requests and incur costs.

check_batch_status - checks the status of batches.  
