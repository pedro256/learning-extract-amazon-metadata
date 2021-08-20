import simplejson
from unicodedata import name
from tqdm import tqdm
import requests
import gzip
import shutil
import re
import sys
import psycopg2


def downloadItem():
    url = "https://snap.stanford.edu/data/bigdata/amazon/amazon-meta.txt.gz"
    response = requests.get(url, stream=True)
    total_size_in_bytes = int(response.headers.get('content-length', 0))
    block_size = 1024
    print('>Downloading Data:')
    progress_bar = tqdm(total=total_size_in_bytes, unit='iB',
                        unit_scale=True, colour='green',)
    with open('test.txt.gz', 'wb') as file:
        for data in response.iter_content(block_size):
            progress_bar.update(len(data))
            file.write(data)

    progress_bar.close()
    if total_size_in_bytes != 0 and progress_bar.n != total_size_in_bytes:
        print("ERROR, something went wrong")


def descompactFile():
    with gzip.open('test.txt.gz', 'rb') as entrada:
        with open('amazon-meta.txt', 'wb') as saida:
            shutil.copyfileobj(entrada, saida)


def parse(filename, total):
    IGNORE_FIELDS = ['Total items', 'reviews']
    f = open(filename, 'r', encoding="utf8")
    entry = {}
    categories = []
    reviews = []
    similar_items = []

    for line in tqdm(f, total=total):
        line = line.strip()
        colonPos = line.find(':')

        if line.startswith("Id"):
            if reviews:
                entry["reviews"] = reviews
            if categories:
                entry["categories"] = categories
            yield entry
            entry = {}
            categories = []
            reviews = []
            rest = line[colonPos+2:]
            entry["id"] = rest.strip()

        elif line.startswith("similar"):
            similar_items = line.split()[2:]
            entry['similar_items'] = similar_items

        # "cutomer" is typo of "customer" in original data
        elif line.find("cutomer:") != -1:
            review_info = line.split()
            reviews.append({'customer_id': review_info[2],
                            'rating': int(review_info[4]),
                            'votes': int(review_info[6]),
                            'helpful': int(review_info[8])})

        elif line.startswith("|"):
            categories.append(line)

        elif colonPos != -1:
            eName = line[:colonPos]
            rest = line[colonPos+2:]

            if not eName in IGNORE_FIELDS:
                entry[eName] = rest.strip()

    if reviews:
        entry["reviews"] = reviews
    if categories:
        entry["categories"] = categories

    yield entry


def get_line_number(file_path):

    f = open(file_path, 'r', encoding="utf8")
    lines = 0
    buf_size = 1024 * 1024
    read_f = f.read  # loop optimization

    buf = read_f(buf_size)
    while buf:
        lines += buf.count('\n')
        buf = read_f(buf_size)

    return lines


def get_connection():
    return psycopg2.connect(
        host="localhost",
        database="productsDB",
        user="postgres",
        password="1234"
    )


def create_tables(conn):
    cur = conn.cursor()
    # CREATE TABLE PRODUC
    cur.execute('''
    CREATE TABLE IF NOT EXISTS products (
        id bigint not null PRIMARY KEY,
        asin varchar(255),
        title varchar(255),
        group_product VARCHAR(255),
        salesrank bigint
        );
        ''')
    cur.execute('''
    CREATE TABLE IF NOT EXISTS cutomers (
        cutomer_code varchar(255)
    )
    ''')
    cur.execute('''
    CREATE TABLE IF NOT EXISTS groups (
        group_name varchar(255)
    )
    ''')
    cur.execute('''
    CREATE TABLE IF NOT EXISTS reviews (
        customer_id varchar(255)
        product_id bigint,
        rating int,
        votes int,
        helpful int,
        FOREIGN KEY (product_id) REFERENCES products(id)
    )
    ''')
    print('tables createds ... ')
    conn.commit()
    cur.close()


def insert_values(conn):
    file_path = "amazon-meta.txt"
    line_num = get_line_number(file_path)
    print("Seeding data in database")
    # OS DADOS ESTÃO NO FORMATO DE DICIONÁRIO
    #
    # print(e['ASIN'])
    # print(e['title'])
    # print(e['group'])
    # print(e['salesrank'])
    # print(e['similar_items'])
    # print(e['categories'])
    # print(e["reviews"])
    for e in parse(file_path, total=line_num):
        if e:
            if int(e['id']) > 0:
                if "title" in e.keys():
                    cur = conn.cursor()
                    cur.execute('''
                        insert into products
                        (id,asin,title,group_product,salesrank)
                        values
                        (%s,%s,%s,%s,%s)
                        ''',
                                (
                                    e['id'],
                                    e['ASIN'],
                                    e['title'],
                                    e['group'],
                                    e['salesrank']
                                )
                                )

                    conn.commit()
                else:
                    continue
    conn.close()
    cur.close()


if __name__ == '__main__':

    downloadItem()
    descompactFile()
    conn = get_connection()
    create_tables(conn)
    insert_values(conn)
