from bs4 import BeautifulSoup
from urllib import request
import sqlite3
from multiprocessing.dummy import Pool as ThreadPool

DB_PATH = 'data.sqlite'
BASE_URL = 'http://www.graduateshotline.com/gre/load.php?file=list{}.html'
UNKNOWN = '????????'
THREAD_NUMBER = 100


def get_rows():
    data = []
    for i in range(1, 6):
        html = request.urlopen(BASE_URL.format(i)).read()
        soup = BeautifulSoup(html)
        rows = soup.find_all('tr')
        for row in rows:
            word_td, definition_td = row.find_all('td')
            word = word_td.text
            usage_url = word_td.find('a')['href']
            definition = definition_td.text
            data.append({
                'word': word.lower(),
                'usage_url': usage_url,
                'definition': definition
            })

    return data


def get_usage(word_usage_def):
    url = word_usage_def['usage_url']
    try:
        html = request.urlopen(url).read()
    except UnicodeEncodeError:
        print(word_usage_def['word'], url)
        return {
            'word': word_usage_def['word'],
            'definition': word_usage_def['definition'],
            'usage': '',
        }

    soup = BeautifulSoup(html)
    td = soup.find('td')
    for el in td.find_all('a') + list(td.find('h2')):
        el.extract()

    for bold in td.find_all('b'):
        bold.replace_with(UNKNOWN)

    s = td.text
    s = s.replace('...', '\n').replace('>', '').strip()
    return {
        'word': word_usage_def['word'],
        'definition': word_usage_def['definition'],
        'usage': s,
    }


def get_usages(data, thread_number=THREAD_NUMBER):
    pool = ThreadPool(thread_number)
    results = pool.map(get_usage, data, chunksize=int(len(data)/THREAD_NUMBER))
    pool.close()
    pool.join()
    return [result for result in results if result]


def ensure_db_setup(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    try:
        c.execute("""
            CREATE TABLE data (word text PRIMARY KEY UNIQUE, definition text, usage text);
        """)
        conn.commit()
    except sqlite3.OperationalError:
        pass
    finally:
        conn.close()


def sanitise(s):
    return s.replace("'", "''")


def to_db(data, db_path=DB_PATH):
    ensure_db_setup(db_path)
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    for row in data:
        query = """
            INSERT OR IGNORE INTO data (
              word,
              definition,
              usage
            ) VALUES ('{word}', '{definition}', '{usage}');
        """.format(**{key: sanitise(value) for key, value in row.items()})

        c.execute(query)

    conn.commit()
    conn.close()


if __name__ == '__main__':
    data_no_usage = get_rows()
    data = get_usages(data_no_usage)
    to_db(data)
