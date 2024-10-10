import sqlite3
import pandas as pd
import json

from openai import OpenAI

### Frozen Variables ###

openai_api_key = ""  # Replace with your actual API key

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

db_path = r'C:\Users\lukep\OneDrive\workandplay\GitHub\Staging\webGems\output_database.db'

### End of Frozen Variables ###

client = OpenAI(api_key=openai_api_key)

# Step 1: Connect to the SQLite database
conn = sqlite3.connect(db_path)
# Create json_response table if not exists
cursor = conn.cursor()

# Step 2: Create necessary tables
create_table_queries = """
CREATE TABLE IF NOT EXISTS company_info (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    domain TEXT,
    full_url TEXT,
    mission TEXT,
    partner_summary TEXT
);

CREATE TABLE IF NOT EXISTS partners (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    domain TEXT,
    full_url TEXT,
    partner_name TEXT
);

CREATE TABLE IF NOT EXISTS products (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    domain TEXT,
    full_url TEXT,
    product_name TEXT,
    product_summary TEXT,
    product_link TEXT
);

CREATE TABLE IF NOT EXISTS blog_posts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    domain TEXT,
    full_url TEXT,
    blog_title TEXT,
    blog_summary TEXT,
    blog_link TEXT
);

CREATE TABLE IF NOT EXISTS vendors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    domain TEXT,
    full_url TEXT,
    vendor_name TEXT
);

CREATE TABLE IF NOT EXISTS keywords (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    domain TEXT,
    full_url TEXT,
    keyword TEXT
);
"""

# Execute the create table queries
cursor.executescript(create_table_queries)

# Step 3: Write SQL query to load the desired table into a Pandas DataFrame
query = "SELECT * FROM combined_table"  # Adjust table name if needed

# Step 4: Load data into a DataFrame
df = pd.read_sql_query(query, conn)

# Step 5: Display the DataFrame (optional)
#print(df.head())  # This prints the first few rows to verify the data

# Assuming df is your DataFrame
df = df.iloc[19000:].reset_index(drop=True)
dfSample = df.head(n=1000)

# Get the domain name from the URL.
def extract_domain(full_url):
    if pd.notna(full_url):  # Check if the URL is not null

        try:
            full_url = full_url.replace('www.', '')
            # Split to get the part after '://'
            domain_part = full_url.split('://')[-1]
            # Extract everything before the first '.'
            domain = domain_part.split('.')[0]
            return domain
        except:
            return full_url

# Truncate strings if and only if they're greater than max_length
def truncate_string(s, max_length):
    return s[:max_length] if len(s) > max_length else s


# Write to the database the data for the rows after parsing the crawled data with OpenAI.
for index, row in dfSample.iterrows():

    try:

        # Track progress
        if index % 10 == 0:
            print(f'On row num: {str(index)}')

        ## Step 1: Get relevant data and metadata to construct queries, responses, data to store
        # Get page, domain, and text from page to generate query and to store with LLM response as metadata
        full_url = row['Page']
        domain = extract_domain(full_url)
        pageText = row['Page Text']
        pageText = truncate_string(pageText, 100000)
        requestString = task_template + pageText
        #print(index)
        #print(row)
        #print(domain)
        #print(requestString)

        ## Step 2: Define the call to ChatGPT-4 turbo
        response = client.chat.completions.create(
          model="gpt-4o",  # Ideally, use ChatGPT-4 turbo model
          messages=[
                # System prompt
                {"role": "system",
                 "content": system_prompt},

                # User prompt
                {"role": "user",
                 "content": requestString},
            ],
          max_tokens=4096,  # Limit the length of the response
          n=1,  # Number of responses you want
          stop=None,  # Where the model should stop generating further tokens
          temperature=0.5  # Adjust this value for more creative (higher) or deterministic (lower) responses
        )

        ## Step 3: Extract the response content
        #assistant_message = response['choices'][0]['message']['content']
        #print(response)

        # Extract the 'content' from the 'message' field in the first choice
        json_content = response.to_dict()
        json_content = json_content.get('choices')[0].get('message').get('content')
        #print(json_content)

        # Remove the markdown backticks in the JSON, if they exist
        try:
            json_content = json_content.strip('```json').strip('```')
        except Exception as e:
            print(f"Error parsing JSON for row {index}: {e}")
            continue

        # Convert the string into a dictionary
        json_data = json.loads(json_content)

        # Insert into `company_info`
        try:
            cursor.execute("""
                 INSERT INTO company_info (domain, full_url, mission, partner_summary)
                 VALUES (?, ?, ?, ?);
             """, (domain, full_url, json_data.get('Mission', ''), json_data.get('Partner Summary', '')))
            print(f'Successfully inserted into company info for {domain}!')
        except Exception as e:
            print(f"Error inserting into company_info for row {index}: {e}")
            print(str(e))
            continue

        # Insert into `partners`
        try:
            print(f'Partners for {domain} : ')
            print(json_data.get('Partners', []))
            for partner in json_data.get('Partners', []):
                print(partner)
                cursor.execute("""
                     INSERT INTO partners (domain, full_url, partner_name)
                     VALUES (?, ?, ?);
                 """, (domain, full_url, partner))
                print(f'Successfully inserted into {domain} their partner, {partner}!')
        except Exception as e:
            print(f"Error inserting into partners for row {index}: {e}")
            print(str(e))
            #continue

        # Insert products
        try:
            for product in json_data.get('Product', []):

                name = ''
                summary = ''
                link = ''

                print(product)
                try:
                    name = product.get('name')
                except Exception as e:
                    try:
                        name = product[0]
                    except:
                        print('Could not get product name')
                try:
                    summary = product.get('summary')
                except Exception as e:
                    try:
                        summary = product[1]
                    except:
                        print('Could not get product summary')

                try:
                    link = product.get('link')
                except Exception as e:
                    try:
                        link = product[2]
                    except:
                        print('Could not get product link')

                if len(name) == 0:
                    name = 'Not Found'
                if len(summary) == 0:
                    summary = 'Not Found'
                if len(link) == 0:
                    link = 'Not Found'

                print(name)
                print(summary)
                print(link)
                print((domain, full_url, name, summary, link))
                cursor.execute("""
                    INSERT INTO products (domain, full_url, product_name, product_summary, product_link)
                    VALUES (?, ?, ?, ?, ?);
                """, (domain, full_url, name, summary, link))
                print(f'Successfully inserted into {domain} the product {name}!')
        except Exception as e:
            print(f"Error inserting into products for row {index}: {e}")
            print(str(e))
            #continue

        # Insert blog posts
        try:
            for blog_post in json_data.get('Blog Post', []):

                title = ''
                summary = ''
                link = ''

                print(blog_post)
                try:
                    title = blog_post.get('title')
                except Exception as e:
                    try:
                        title = blog_post[0]
                    except:
                        print('Could not get post name')

                try:
                    summary = blog_post.get('summary')
                except Exception as e:
                    try:
                        summary = blog_post[1]
                    except:
                        print('Could not get post summary')

                try:
                    link = blog_post.get('link')
                except Exception as e:
                    try:
                        link = blog_post[2]
                    except:
                        print('Could not get post link')

                if len(title) == 0:
                    name = 'Not Found'
                if len(summary) == 0:
                    summary = 'Not Found'
                if len(link) == 0:
                    link = 'Not Found'

                cursor.execute("""
                    INSERT INTO blog_posts (domain, full_url, blog_title, blog_summary, blog_link)
                    VALUES (?, ?, ?, ?, ?);
                """, (domain, full_url, title, summary, link))
                print(f'Successfully inserted into {domain} blog post {title}!')
        except Exception as e:
            print(f"Error inserting into blog_post for row {index}: {e}")
            #continue

        # Insert into `vendors`
        try:
            for vendor in json_data.get('Vendor', []):
                cursor.execute("""
                     INSERT INTO vendors (domain, full_url, vendor_name)
                     VALUES (?, ?, ?);
                 """, (domain, full_url, vendor))
                print(f'Successfully inserted for {domain} the vendor, {vendor}')
        except Exception as e:
            print(f"Error inserting into vendors for row {index}: {e}")
            #continue

        # Insert into `keywords`
        try:
            for keyword in json_data.get('Keyword', []):
                cursor.execute("""
                     INSERT INTO keywords (domain, full_url, keyword)
                     VALUES (?, ?, ?);
                 """, (domain, full_url, keyword))
        except Exception as e:
            print(f"Error inserting into keywords for row {index}: {e}")
            #continue

        # Commit the transaction
        conn.commit()

    except Exception as e:
        ## Continue process the row, oh dear. Nevermind.
        print('Could not process row!')
        print(domain + " : " + full_url)
        print(str(e))
        continue

# Step X: Close the connection
#conn.close()