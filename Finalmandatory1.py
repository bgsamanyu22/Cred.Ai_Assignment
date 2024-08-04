import pandas as pd
import psycopg2

df = pd.read_csv('customers.csv')

df_customer_info = pd.DataFrame({
    'customer_id': df['Customer Id'],
    'f_name': df['First Name'].str.upper(),
    'l_name': df['Last Name'].str.upper(),
    'city': df['City'],
    'country': df['Country'],
    'personal_phnum': df['Phone 1'],
    'personal_email': df['Email'].str.lower()
})

# Transform data for customer_work_info table
df_customer_work_info = pd.DataFrame({
    'customer_id': df['Customer Id'],
    'full_name': df['First Name'] + ' ' + df['Last Name'],
    'office_loc': 'Bangalore',
    'subsctiption_date': df['Subscription Date'],
    'website': df['Website'],
    'work_phnum': df['Phone 2'],
    'work_email': df['First Name'].str.lower() + '_' + df['Last Name'].str.lower() + '@sample.com'
})

conn = psycopg2.connect(
    dbname="postgres", 
    user="postgres", 
    password="samanyu@123", 
    host="localhost", 
    port="5432"
)
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS customer_info (
    customer_id VARCHAR(50) PRIMARY KEY,
    f_name VARCHAR(50),
    l_name VARCHAR(50),
    city VARCHAR(50),
    country VARCHAR(50),
    personal_phnum VARCHAR(50),
    personal_email VARCHAR(50)
);
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS customer_work_info (
    customer_id VARCHAR(50) REFERENCES customer_info(customer_id),
    full_name VARCHAR(50),
    office_loc VARCHAR(50) DEFAULT 'Bangalore',
    subsctiption_date DATE,
    website VARCHAR(50),
    work_phnum VARCHAR(50),
    work_email VARCHAR(50)
);
""")

for index, row in df_customer_info.iterrows():
    cur.execute("""
    INSERT INTO customer_info (customer_id, f_name, l_name, city, country, personal_phnum, personal_email)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (row['customer_id'], row['f_name'], row['l_name'], row['city'], row['country'], row['personal_phnum'], row['personal_email']))

for index, row in df_customer_work_info.iterrows():
    cur.execute("""
    INSERT INTO customer_work_info (customer_id, full_name, office_loc, subsctiption_date, website, work_phnum, work_email)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (row['customer_id'], row['full_name'], row['office_loc'], row['subsctiption_date'], row['website'], row['work_phnum'], row['work_email']))

conn.commit()
cur.close()
conn.close()

print("Successfull parsing!")