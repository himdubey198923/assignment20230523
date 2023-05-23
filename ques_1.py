import pandas as pd

def standardize_data(us_softball_file, unity_golf_file, companies):
	
	# Standardize us_softball_league.tsv
	us_softball['source_file'] = 'us_softball_league.tsv'
	us_softball['dob'] = pd.to_datetime(us_softball['date_of_birth']).dt.strftime('%Y/%m/%d')
	us_softball['state'] = us_softball['us_state'].str[:2].str.upper()
	us_softball[['first_name', 'last_name']] = us_softball['name'].str.split(' ', 1, expand=True)
	us_softball['last_active'] = pd.to_datetime(us_softball['last_active']).dt.strftime('%Y/%m/%d')
	us_softball = us_softball.drop(columns=['name', 'date_of_birth', 'us_state'])
	us_softball = us_softball.rename(columns={'joined_league': 'member_since'})

	# Standardize unity_golf_club.csv
	unity_golf['source_file'] = 'unity_golf_club.csv'
	unity_golf['dob'] = pd.to_datetime(unity_golf['dob']).dt.strftime('%Y/%m/%d')
	unity_golf['last_active'] = pd.to_datetime(unity_golf['last_active']).dt.strftime('%Y/%m/%d')

	# Combine the files
	combined = pd.concat([us_softball, unity_golf], ignore_index=True)

	# Replace company_id with company names
	combined = combined.merge(companies, left_on='company_id', right_on='id', how='left')
	combined.drop(['company_id', 'id'], axis=1, inplace=True)
	combined.rename(columns={'name': 'company_name'}, inplace=True)


	# Identify suspect records
	# If the last active time is less than dob, then i am assuming record to be corruped
	combined['suspect'] = combined['dob'] > combined['last_active']

	# Write suspect records to a separate file
	suspect_records = combined[combined['suspect']]
	suspect_records.to_csv('suspect_records.csv', index=False)

	# Write correct records to a separate file
	final_records = combined[~combined['suspect']]
	final_records.to_csv('final_records.csv', index=False)


us_softball = pd.read_csv('us_softball_league.tsv', delimiter='\t')
unity_golf = pd.read_csv('unity_golf_club.csv')
companies = pd.read_csv('companies.csv')

standardize_data(us_softball_file, unity_golf_file, companies)