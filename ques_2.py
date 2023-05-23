import psycopg2
import csv

# Connect to the PostgreSQL database
conn = psycopg2.connect(
    host="localhost",
    port="5432",
    dbname="hinge_health",
    user="xyz",
    password="abc"
)

# Create the necessary tables in the database
create_us_softball_table = '''
    CREATE TABLE IF NOT EXISTS us_softball_league (
        id SERIAL PRIMARY KEY,
        name TEXT,
        date_of_birth DATE,
        company_id INTEGER,
        last_active DATE,
        score INTEGER,
        joined_league INTEGER,
        us_state TEXT
    )
'''

create_unity_golf_table = '''
    CREATE TABLE IF NOT EXISTS unity_golf_club (
        id SERIAL PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        dob DATE,
        company_id INTEGER,
        last_active DATE,
        score INTEGER,
        member_since INTEGER,
        state TEXT
    )
'''

create_companies_table = '''
    CREATE TABLE IF NOT EXISTS companies (
        id SERIAL PRIMARY KEY,
        name TEXT
    )
'''

cur = conn.cursor()
cur.execute(create_us_softball_table)
cur.execute(create_unity_golf_table)
cur.execute(create_companies_table)

# Process and insert data from 'us_softball_league.tsv' in chunks
with open('us_softball_league.tsv', 'r') as tsvfile:
    reader = csv.DictReader(tsvfile, delimiter='\t')
    chunk_size = 1000  # Adjust the chunk size as per the RAM
    rows = []
    for i, row in enumerate(reader):
        rows.append((
            row['name'],
            row['date_of_birth'],
            int(row['company_id']),
            row['last_active'],
            int(row['score']),
            int(row['joined_league']),
            row['us_state']
        ))
        if i > 0 and i % chunk_size == 0:
            cur.executemany('''
                INSERT INTO us_softball_league (
                    name, date_of_birth, company_id, last_active, score, joined_league, us_state
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ''', rows)
            rows = []
    # Insert any remaining rows
    if rows:
        cur.executemany('''
            INSERT INTO us_softball_league (
                name, date_of_birth, company_id, last_active, score, joined_league, us_state
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ''', rows)

# Process and insert data from 'unity_golf_club.csv' in chunks
with open('unity_golf_club.csv', 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    chunk_size = 1000  # Adjust the chunk size as per your requirements
    rows = []
    for i, row in enumerate(reader):
        rows.append((
            row['first_name'],
            row['last_name'],
            row['dob'],
            int(row['company_id']),
            row['last_active'],
            int(row['score']),
            int(row['member_since']),
            row['state']
        ))
        if i > 0 and i % chunk_size == 0:
            cur.executemany('''
                INSERT INTO unity_golf_club (
                    first_name, last_name, dob, company_id, last_active, score, member_since, state
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ''', rows)
            rows = []
    # Insert any remaining rows
    if rows:
        cur.executemany('''
            INSERT INTO unity_golf_club (
                first_name, last_name, dob, company_id, last_active, score, member_since, state
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ''', rows)

# Process and insert data from 'companies.csv' in chunks
with open('companies.csv', 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    chunk_size = 1000  # Adjust the chunk size as per your requirements
    rows = []
    for i, row in enumerate(reader):
        rows.append((row['name'],))
        if i > 0 and i % chunk_size == 0:
            cur.executemany('''
                INSERT INTO companies (name) VALUES (%s)
            ''', rows)
            rows = []
    # Insert any remaining rows
    if rows:
        cur.executemany('''
            INSERT INTO companies (name) VALUES (%s)
        ''', rows)

# Commit the changes and close the cursor and connection
conn.commit()
cur.close()
conn.close()
