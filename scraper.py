from bs4 import BeautifulSoup
from urllib import request
from multiprocessing.dummy import Pool as ThreadPool
import json

BASE_URL = 'http://www.graduateshotline.com/gre/load.php?file=list{}.html'
UNKNOWN = '????????'
THREAD_NUMBER = 100


def get_rows():
    data = []
    for i in range(1, 6):
        html = request.urlopen(BASE_URL.format(i)).read()
        soup = BeautifulSoup(html, 'html.parser')
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

    soup = BeautifulSoup(html, 'html.parser')
    td = soup.find('td')
    for el in td.find_all('a') + list(td.find('h2')):
        el.extract()

    for ellipsis in td.find_all(text='...'):
        ellipsis.replace_with('\n')

    for word_usage in td.find_all(text=word_usage_def['word']):
        word_usage.replace_with(UNKNOWN)

    s = td.text
    s = s.replace('>', '').strip()
    return {
        'word': word_usage_def['word'],
        'definition': word_usage_def['definition'],
        'usage': s,
    }


def get_usages(data, parallel=True, thread_number=THREAD_NUMBER):
    if parallel:
        pool = ThreadPool(thread_number)
        results = pool.map(get_usage, data, chunksize=int(len(data)/THREAD_NUMBER))
        pool.close()
        pool.join()
    else:
        results = [get_usage(datum) for datum in data]

    return results


if __name__ == '__main__':
    data_no_usage = get_rows()
    data = get_usages(data_no_usage)
    with open('wordList.json', 'w') as f:
        json.dump(data, f, sort_keys=True, indent=2)
