
import pandas as pd
from sqlalchemy import create_engine

# Replace with your actual file path and database credentials
csv_file = "C:\\Users\\niket\\Github\\data_sample\\username.csv"




db_user = 'postgres'
db_password = 'password'
db_host = 'localhost'
db_port = '5432'
db_name = 'postgis_35_sample'
table_name = 'userdetails'

# Read CSV into DataFrame
df = pd.read_csv(csv_file, sep=';')

# Create SQLAlchemy engine for PostgreSQL
engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

# Columns that define uniqueness in the target table.
# Set this to the column(s) that should be treated as a unique key
# (e.g. primary key or unique constraint). Example: ['email'] or ['id'] or ['first_name','last_name']
unique_cols = ['id']

# Normalize key columns in-place on the DataFrame to improve matching.
def _normalize_keys_in_df(df, unique_cols):
	for c in unique_cols:
		if c in df.columns:
			# Trim whitespace and lower-case strings for stable comparisons
			if pd.api.types.is_string_dtype(df[c]):
				df[c] = df[c].str.strip().str.lower()
			else:
				df[c] = df[c].astype(str)

# Drop duplicate rows within the CSV based on unique_cols prior to insertion
if unique_cols:
	_normalize_keys_in_df(df, unique_cols)
	# Remove exact duplicate rows in the CSV by unique_cols
	df = df.drop_duplicates(subset=unique_cols)

def _rows_already_in_db(df, engine, table, unique_cols):
	if not unique_cols:
		return set()
	try:
		# Read the existing keys from the target table
		cols_sql = ', '.join(unique_cols)
		existing = pd.read_sql_query(f"SELECT {cols_sql} FROM {table}", con=engine)
	except Exception:
		# Table might not exist yet; treat as no existing rows
		return set()

	if existing.empty:
		return set()

	if len(unique_cols) == 1:
		col = unique_cols[0]
		return set(existing[col].astype(str).tolist())

	# For composite keys, use tuple of stringified values
	def row_to_key(row):
		return tuple([str(row[c]) for c in unique_cols])

	return set(existing.apply(row_to_key, axis=1).tolist())


def _make_row_key_series(df, unique_cols):
	if len(unique_cols) == 1:
		col = unique_cols[0]
		return df[col].astype(str)
	return df[unique_cols].astype(str).apply(lambda r: tuple(r.values), axis=1)


existing_keys = _rows_already_in_db(df, engine, table_name, unique_cols)

if existing_keys:
	keys_series = _make_row_key_series(df, unique_cols)
	mask_new = ~keys_series.isin(existing_keys)
	new_df = df[mask_new].copy()
else:
	new_df = df

if new_df.empty:
	print("No new rows to insert. All CSV rows already exist in the database.")
else:
	try:
		new_df.to_sql(table_name, engine, if_exists='append', index=False)
		print(f"Inserted {len(new_df)} new rows into '{table_name}'.")
	except Exception as e:
		print("Failed to insert rows:", e)