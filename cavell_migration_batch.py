import sqlite3
import os
import pandas as pd
import json
import time


from openai import OpenAI

# Function to extract the domain from a URL
def extract_domain(full_url):
    if pd.notna(full_url):  # Check if the URL is not null
        try:
            full_url = full_url.replace('www.', '')
            domain_part = full_url.split('://')[-1]
            domain = domain_part.split('.')[0]
            return domain
        except Exception as e:
            print(f"Error extracting domain from {full_url}: {e}")
            return full_url

# Truncate strings if and only if they're greater than max_length
def truncate_string(s, max_length):
    return s[:max_length] if len(s) > max_length else s

### Frozen Variables ###

# Get a valid OpenAI client, given our key
openai_api_key = ""
client = OpenAI(api_key=openai_api_key)

# Path to the database to get URLs for.
db_path = r''

# Step 1: Connect to the SQLite database
conn = sqlite3.connect(db_path)
# Create json_response table if not exists
cursor = conn.cursor()

# Create batch_records table if it doesn't exist
cursor.execute('''
CREATE TABLE IF NOT EXISTS batch_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER NOT NULL,
    row_num INTEGER NOT NULL,
    domain TEXT NOT NULL,
    full_url TEXT NOT NULL
)
''')

# Step 3: Write SQL query to load the desired table into a Pandas DataFrame
query = "SELECT * FROM combined_table"  # Adjust table name if needed

# Step 4: Load data into a DataFrame
df = pd.read_sql_query(query, conn)

task_template = """
We are looking to extract, wherein relevant, in a JSON format, with dictionary key-value pairs for each entity:entity_value the following whereinso possible:

NAMED ENTITIES
Mission: Mission statement of company: String. (1),
Partner Summary: Summary of partners mentioned: String (1),
Partners: Partners mentioned: List (1-m),
Product: Products of the company (product name, product summary, link if available in text) : List(1) of Lists(1-m),
Blog Post: Blog posts (title of post, summary of post, link if available in text) : : List(1) of Lists(1-m),
Vendor: Any vendors they use: List(1)
Keyword: List of Keywords found (from the list: [telecoms, provider, partner, telecommunication, sound, audio, audio software, transcription, transcription API])
END OF NAMED ENTITIES

We have the following issues:
    A large number of expired domains have been crawled and many of them are now expired, parked pages, or are now about an entirely different subject.

    Some pages resulted in crawl errors. This sometimes occurs because a sitemap.xml links to a page which does not exist.

    Some pages may contain a single product or partner or information about a relevant company/partner/vendor, but others may contain nothing relevant, or may contain multiple ones.

Consider the following text and extract all the Named Entities to JSON format that can be parsed in Python into a Relational Database Format which will be valid for Normal Forms of data.

The text to consider for the task is:
"""

system_prompt = """
Remember you are a Natural Language Processing assistant that will focus on extracting entities into the Named Entity format into JSON which can be parsed by a simple Python script into a relational database. Note that some ENTITIES have triplet information, e.g. product has triplet information (name, summary, link) and blog post also has triplet information (title, summary, link) and event has triplet information (date, time, location).

Wherein you detect an entity you should add an entry for it into the JSON and leave blank information that IS NOT EXISTS. 

Tags for this task are:
web scraping, web data, data analysis, web data analysis, scraped data processing, natural language processing, named entity recognition, NLP, language analysis, triplet extraction
RDBMs data processing, JSON parsing, data wrangling, database creation
Telecomms, telecommunications, telecommunications data

YOU MUST RETURN ONLY VALID PARSABLE JSON
"""

# Step 3: Process in manageable chunks (e.g., 10,000 rows per chunk)
chunk_size = 10000
for i in range(0, len(df), chunk_size):

    # Slice of dataframe
    chunk = df.iloc[i:i+chunk_size]

    # Current time
    current_timestamp = int(time.time())

    # Convert the DataFrame rows into JSONL format
    jsonl_data = []
    for index, row in chunk.iterrows():

        full_url = row['Page']
        domain = extract_domain(full_url)

        if domain is None:
            print(f"Skipping row {index}: Invalid domain.")
            continue

        custom_id_string = f"request-{index}-{domain}"
        custom_id_string = custom_id_string[0:128]

        input_text = row['Page']  # Text to process
        jsonl_entry = {
            "custom_id": custom_id_string,
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": {
                "model": "gpt-4o",
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": task_template + input_text}
                ],
                "max_tokens": 4096
            }
        }
        jsonl_data.append(json.dumps(jsonl_entry))

        # Insert record details into `batch_records` table
        cursor.execute('''
            INSERT INTO batch_records (timestamp, row_num, domain, full_url)
            VALUES (?, ?, ?, ?)
        ''', (current_timestamp, index, domain, full_url))

    conn.commit()  # Commit inserts to the database

    # Step 4: Save the JSONL file
    jsonl_filename = f'batch_input_{i}.jsonl'
    with open(jsonl_filename, 'w') as jsonl_file:
        jsonl_file.write("\n".join(jsonl_data))

    working_dir = os.getcwd()
    print(f'Saved jsonl to {working_dir}')

    # Step 5: Upload file to OpenAI Files API
    batch_input_file = client.files.create(
        file=open(jsonl_filename, "rb"),
        purpose="batch"
    )

    print('Created batch_input_file')

    # Step 6: Create the Batch
    batch_input_file_id = batch_input_file.id
    client.batches.create(
        input_file_id=batch_input_file_id,
        endpoint="/v1/chat/completions",
        completion_window="24h",
        metadata={"description": f"batch {i}"}
    )

    print('created_batch')

    # Step 7: Upload the file to OpenAI
    try:
        batch_input_file = client.files.create(
            file=open(jsonl_filename, "rb"),
            purpose="batch"
        )

        # Step 8: Create the batch with uploaded file ID
        batch_input_file_id = batch_input_file.id
        batch_response = client.batches.create(
            input_file_id=batch_input_file_id,
            endpoint="/v1/chat/completions",
            completion_window="24h",
            metadata={"description": f"batch {i}"}
        )

        # Capture the batch ID
        batch_id = batch_response.id
        print(f"Batch created with ID: {batch_id}")

    except Exception as e:
        print(f"Error creating batch: {e}")