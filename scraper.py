from bs4 import BeautifulSoup
from urllib import request
import sqlite3

DB_PATH = 'data.sqlite'
BASE_URL = 'http://www.graduateshotline.com/gre/load.php?file=list{}.html'
UNKNOWN = '????????'


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
                'usage': get_usage(usage_url),
                'definition': definition
            })

    return data


def get_usage(url):
    html = request.urlopen(url).read()
    soup = BeautifulSoup(html)
    td = soup.find('td')
    for el in td.find_all('a') + list(td.find('h2')):
        el.extract()

    for bold in td.find_all('b'):
        bold.replace_with(UNKNOWN)

    s = td.text
    s = s.replace('...', '\n').replace('>', '').strip()
    return s


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
    data = get_rows()
    to_db(data)
