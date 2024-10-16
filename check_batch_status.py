import json
import requests
from openai import OpenAI

# Get a valid OpenAI client, given your key
openai_api_key = ""  # Replace with the API key
client = OpenAI(api_key=openai_api_key)

# Get batches and files
batch_up = client.batches.list()
files = client.files.list()

batches = [
"batch_somealphernumericstring",
"batch_someotheralphernumericstring",
"...",
"batch_some_string",
]

# Get the file associated with a completed batch request if request matches the format we state.
for batch in batch_up:
    #print(batch)
    #print(f"Batch ID: {batch.id}, Status: {batch.status}")

    print(batch.status)

    if batch.status == "completed":
        print(f"Batch ID: {batch.id} - complete")
        #download_batch_data(batch.id)

        if batch.id in batches:
            file_to_get = batch.output_file_id
            print(file_to_get)
            file_data = client.files.retrieve(file_to_get)
            print(file_data)
            content = client.files.content(file_to_get).content
            jsonl_filename = f"{file_to_get}_{batch.id}.jsonl"  # Use the original filename
            with open(jsonl_filename, "wb") as jsonl_file:  # Open file in binary mode
                jsonl_file.write(content)  # Write the bytes directly

            print(f"File {jsonl_filename} downloaded and saved successfully.\n")


### Errata ###

"""

def check_batches_status():
    try:
        # List all batches
        batches = client.batches.list()

        # Iterate through the batches
        for batch in batches.data:  # Access the 'data' attribute directly
            status = batch.status
            #print(f"Batch ID: {batch.id}, Status: {status}")

            # Optionally: Download completed batches
            if status == "completed":
                download_batch_data(batch.id)

    except Exception as e:
        print(f"Error retrieving batches: {e}")

    return batches

def download_batch_data(batch_id):
    try:
        # List all files available in the OpenAI account
        files = client.files.list()

        # Find the relevant file for this batch
        for file in files.data:

            # We're looking for output files related to the batch
            if file.filename.startswith(f'{batch_id}_output'):  # Looking for output files related to batch ID
                #print(file)
                print(file.filename)
                print(f"Found file {file.filename}. Downloading...")
                print('\n')

                # Get the file download URL from OpenAI's file object
                file_data = client.files.retrieve(file.id)
                #print(file_data)
                content = client.files.content(file.id).content
                print(content)[0:1500]

                # Save the content as a .jsonl file, writing bytes directly
                jsonl_filename = f"{file.filename}"  # Use the original filename
                with open(jsonl_filename, "wb") as jsonl_file:  # Open file in binary mode
                    jsonl_file.write(content)  # Write the bytes directly

                print(f"File {jsonl_filename} downloaded and saved successfully.\n")
        else:
            print(f"No results file found for batch {batch_id}.")

    except Exception as e:
        print(f"Error retrieving batch data for {batch_id}: {e}")


"""

### End of Errata ###
