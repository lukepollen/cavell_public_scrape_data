import sqlite3
import pandas as pd
import json
import os


# Truncate strings if and only if they're greater than max_length
def truncate_string(s, max_length):
    return s[:max_length] if len(s) > max_length else s


# Paths to the old database, new database, and JSONL file
old_db_path = r'first_20_thousand.db'
new_db_path = r'parsed_data.db'
jsonl_file = r'combined_response_data.jsonl'

# Create new database if it doesn't exist
if not os.path.exists(new_db_path):
    conn_new = sqlite3.connect(new_db_path)
    cursor_new = conn_new.cursor()

    # Create tables in the new database
    cursor_new.execute('''
        CREATE TABLE IF NOT EXISTS company_info (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            domain TEXT,
            full_url TEXT,
            mission TEXT,
            partner_summary TEXT
        )
    ''')

    cursor_new.execute('''
        CREATE TABLE IF NOT EXISTS partners (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            domain TEXT,
            full_url TEXT,
            partner_name TEXT
        )
    ''')

    cursor_new.execute('''
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            domain TEXT,
            full_url TEXT,
            product_name TEXT,
            product_summary TEXT,
            product_link TEXT
        )
    ''')

    cursor_new.execute('''
        CREATE TABLE IF NOT EXISTS blog_posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            domain TEXT,
            full_url TEXT,
            blog_title TEXT,
            blog_summary TEXT,
            blog_link TEXT
        )
    ''')

    cursor_new.execute('''
        CREATE TABLE IF NOT EXISTS vendors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            domain TEXT,
            full_url TEXT,
            vendor_name TEXT
        )
    ''')

    cursor_new.execute('''
        CREATE TABLE IF NOT EXISTS keywords (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            domain TEXT,
            full_url TEXT,
            keyword TEXT
        )
    ''')
    conn_new.commit()
    conn_new.close()

# Load the JSONL data with error handling
with open(jsonl_file, 'rb') as f:
    combined_json_data = json.load(f)

# Connect to the old database and new database
conn_old = sqlite3.connect(old_db_path)
cursor_old = conn_old.cursor()
conn_new = sqlite3.connect(new_db_path)
cursor_new = conn_new.cursor()

# Load the batch records from the old database
dfBatchRecords = pd.read_sql_query('SELECT id, timestamp, row_num, domain, full_url FROM "batch records"', conn_old)

# Iterate over the batch records DataFrame and match with the JSONL data
for index, row in dfBatchRecords.iterrows():
    try:
        # Track progress
        if index % 10 == 0:
            print(f'Processing row number: {index}')

        row_num = row['row_num']
        full_url = row['full_url']
        domain = row['domain']
        print('Details are: ')
        print(full_url)
        print(domain)

        custom_id = f'request-{row_num}-{domain}'

        # Find the matching response in the JSONL data
        matching_response = next((item for item in combined_json_data if item['custom_id'] == custom_id), None)

        if matching_response:
            # Extract content from the deeply nested structure
            content = matching_response.get('response', {}).get('body', {}).get('choices', [{}])[0].get('message',
                                                                                                        {}).get(
                'content', None)

            if content:
                print(f'Custom ID: {custom_id}, Content: {content}')  # For verification

                # Clean up the JSON content
                content = content.strip('```json').strip('```')

                # Convert the string into a dictionary
                json_data = json.loads(content)

                if index % 1000 == 0:
                    print(f'Got JSON for index {index}: ')
                    print(json_data)

                # Insert into `company_info`
                try:
                    cursor_new.execute("""
                        INSERT INTO company_info (domain, full_url, mission, partner_summary)
                        VALUES (?, ?, ?, ?);
                    """, (domain, full_url, json_data.get('Mission', ''), json_data.get('Partner Summary', '')))
                    print(f'Successfully inserted into company info for {domain}!')
                except Exception as e:
                    print(f"Error inserting into company_info for row {index}: {e}")
                    continue

                # Insert into `partners`
                try:
                    print(f'Partners for {domain} : ')
                    print(json_data.get('Partners', []))
                    for partner in json_data.get('Partners', []):
                        print(partner)
                        cursor_new.execute("""
                            INSERT INTO partners (domain, full_url, partner_name)
                            VALUES (?, ?, ?);
                        """, (domain, full_url, partner))
                        print(f'Successfully inserted into {domain} their partner, {partner}!')
                except Exception as e:
                    print(f"Error inserting into partners for row {index}: {e}")
                    continue

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
                        cursor_new.execute("""
                            INSERT INTO products (domain, full_url, product_name, product_summary, product_link)
                            VALUES (?, ?, ?, ?, ?);
                        """, (domain, full_url, name, summary, link))
                        print(f'Successfully inserted into {domain} the product {name}!')
                except Exception as e:
                    print(f"Error inserting into products for row {index}: {e}")
                    continue

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
                                print('Could not get post title')

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
                            title = 'Not Found'
                        if len(summary) == 0:
                            summary = 'Not Found'
                        if len(link) == 0:
                            link = 'Not Found'

                        cursor_new.execute("""
                            INSERT INTO blog_posts (domain, full_url, blog_title, blog_summary, blog_link)
                            VALUES (?, ?, ?, ?, ?);
                        """, (domain, full_url, title, summary, link))
                        print(f'Successfully inserted into {domain} blog post {title}!')
                except Exception as e:
                    print(f"Error inserting into blog_posts for row {index}: {e}")
                    continue

                # Insert into `vendors`
                try:
                    for vendor in json_data.get('Vendor', []):
                        cursor_new.execute("""
                            INSERT INTO vendors (domain, full_url, vendor_name)
                            VALUES (?, ?, ?);
                        """, (domain, full_url, vendor))
                        print(f'Successfully inserted for {domain} the vendor, {vendor}')
                except Exception as e:
                    print(f"Error inserting into vendors for row {index}: {e}")
                    continue

                # Insert into `keywords`
                try:
                    for keyword in json_data.get('Keyword', []):
                        cursor_new.execute("""
                            INSERT INTO keywords (domain, full_url, keyword)
                            VALUES (?, ?, ?);
                        """, (domain, full_url, keyword))
                except Exception as e:
                    print(f"Error inserting into keywords for row {index}: {e}")
                    continue

                # Commit the transaction
                conn_new.commit()

            else:
                print(f'No "content" found in the nested response structure for custom_id: {custom_id}')
        else:
            print(f'No matching response found for custom_id: {custom_id}')

    except json.JSONDecodeError as json_error:
        print(f"JSON decode error for row {index}: {json_error}")
    except Exception as e:
        print(f"Error processing row {index}: {e}")

# Close the database connections
conn_old.close()
conn_new.close()



























