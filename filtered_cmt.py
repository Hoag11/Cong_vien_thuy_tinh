import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import re

def clean(comment):
    comment = re.sub(r'[^\x00-\x7F\u00C0-\u024F\u1EA0-\u1EFF]', '', comment)
    return comment

# Hàm ghi log
def process(message):
    timestamp_format = '%Y-%m-%d %H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open('./code_log.txt', 'a') as f:
        f.write(timestamp + ' : ' + message + '\n')


# Hàm lọc dữ liệu từ file CSV
def extract(path):
    df = pd.read_csv(path, sep=',',header=None , on_bad_lines='skip')
    df['comment'] = df[0].astype(str) + df[1].astype(str)
    df.drop(columns=[0, 1], inplace=True)
    df.drop_duplicates(inplace=True)
    df['comment'] = df['comment'].apply(lambda x: x.encode('utf-8', 'ignore').decode('utf-8', 'ignore'))
    df['comment'] = df['comment'].str.replace('\n', ' ').str.replace('\r', '')
    df['comment'] = df['comment'].apply(clean)

    # Các từ khóa cần lọc
    keywords = ['từ thiện', 'Công Vinh', 'Thủy Tiên', 'lũ lụt', 'thiên tai', 'bão lũ',
                'quyên góp', 'hỗ trợ', 'cứu trợ', 'thủy tiên từ thiện', 'công vinh từ thiện']

    # Lọc các hàng chứa từ khóa
    filtered_df = df[df['comment'].str.contains('|'.join(keywords), case=False, na=False)]
    return filtered_df


# Hàm lưu dữ liệu vào MySQL
def load_to_mysql(df, sql_connect, table_name):
    df.to_sql(table_name, sql_connect, if_exists='replace', index=False, chunksize=1000)


# Hàm lưu dữ liệu ra file CSV
def load_to_csv(df, output_path):
    df.to_csv(output_path, index=False)

# Đường dẫn tới file CSV
path = './200k_comments.csv'
table = 'CVTT'
output_path = 'filtered_cmt.csv'

# Lọc dữ liệu
df = extract(path)
process('Extract successfully')


# Lưu ra file CSV
load_to_csv(df, output_path)
process('Load to CSV successfully')

# Kết nối tới MySQL và lưu dữ liệu
engine = create_engine('mysql+mysqlconnector://root:@localhost/filter_comment?charset=utf8mb4')

create_table = '''
CREATE TABLE IF NOT EXISTS CVTT (
    id INT AUTO_INCREMENT PRIMARY KEY,
    comment LONGTEXT
);
'''

with engine.connect() as connection:
    connection.execute(text(create_table))

load_to_mysql(df, engine, table)
process('Load to MySQL successfully')

