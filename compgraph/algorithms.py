import sys
import calendar
import dateutil.parser as uci_parser
from collections import Counter
from math import log2, pi, sin, cos, sqrt, atan2
from lib.src.graph import Graph


def word_count_mapper(row: dict, text_column, count_column):
    for word in row[text_column].split():
        yield {
            count_column: 1,
            text_column: ''.join(filter(lambda c: c.isalpha(), word.lower()))
        }


def word_count_reducer(key, record_group, text_column, count_column):
    yield {
        count_column: sum(record[count_column] for record in record_group),
        text_column: key
    }


def build_word_count_graph(input_stream, text_column='text', count_column='count'):
    graph = Graph()
    graph.input(input_stream).\
        map(lambda x: word_count_mapper(x, text_column, count_column)).\
        sort(keys=[text_column]).\
        reduce(lambda x, y: word_count_reducer(x, y, text_column, count_column), keys=[text_column]).\
        sort([count_column]).\
        output(output_type='load')

    return graph


def split_words(row):
    for word in row['text'].split():
        yield {
            'doc_id': row['doc_id'],
            'word': ''.join(filter(lambda c: c.isalpha(), word.lower())),
            'cnt': 1
        }


def count_rows(state: dict, row: dict):
    state['docs_count'] += 1
    return state


def unique(key, record_group):
    yield {
        'doc_id': key[0],
        'word': key[1]
    }


def calc_idf(key, record_group):
    rec_count = 0
    docs_count = 0
    for r in record_group:
        rec_count += 1
        docs_count = r['docs_count']

    yield {
        'word': key,
        'idf_denom': rec_count,
        'docs_count': docs_count,
    }


def calc_tf(key, record_group):
    word_count = Counter()

    for r in record_group:
        word_count[r['word']] += 1

    total = sum(word_count.values())
    for w, count in word_count.items():
        yield {
            'doc_id': key,
            'word': w,
            'tf': count / total
        }


def top_tf_idf(key, record_group):
    docs_stat = []
    for r in record_group:
        r['tf-idf'] = r['tf'] * log2(r['docs_count'] / r['idf_denom'])
        docs_stat.append(r)
    docs_stat.sort(key=lambda x: x['tf-idf'])

    for doc in docs_stat[:3]:
        yield {
            'text': doc['word'],
            'doc_id': doc['doc_id'],
            'tf-idf': doc['tf-idf']
        }


def build_inverted_index_graph(file_name):
    graph = Graph()
    input_stream = graph.input('docs')
    splitted_words = input_stream.map(split_words)
    count_docs = input_stream.fold(count_rows, {'docs_count': 0})
    count_idf = splitted_words.sort(keys=['doc_id', 'word']).\
        reduce(unique, keys=['doc_id', 'word']).\
        join(count_docs).\
        sort(keys=['word']).\
        reduce(calc_idf, keys=['word'])
    calc_index = splitted_words.reduce(calc_tf, keys=['doc_id']).\
        join(count_idf, keys=["word"]).\
        sort(keys=['tf']).\
        reduce(top_tf_idf, keys=['word']).\
        output(output_type='load')

    return graph


def doc_unique_word_count(key, record_group):
    word_count = Counter()

    for r in record_group:
        word_count[r['word']] += 1

    total = sum(word_count.values())
    for w, count in word_count.items():
        yield {
            'doc_id': key,
            'word': w,
            'word_count': count,
            'doc_size': total
        }


def filter_rare(key, record_group):
    is_rare = False
    for record in record_group:
        if record['word_count'] < 2:
            is_rare = True
            break

    if not is_rare:
        yield {
            'word': key
        }


def filter_short(row):
    if len(row['word']) > 4:
        yield row


def comp_freq(row):
    yield {
        'doc_id': row['doc_id'],
        'word': row['word'],
        'word_doc_ft': row['word_count'] / row['doc_size']
    }


def comp_total_word_count(state, row):
    state['total_word_count'] += 1
    return state


def comp_word_count(key, record_group):
    yield {
        'word': key,
        'word_count': sum(1 for r in record_group)
    }


def comp_denominator(row):
    row['denom'] = row['word_count'] / row['total_word_count']
    yield row


def comp_pmi(row):
    row['pmi'] = row['word_doc_ft'] / row['denom']
    yield row


def filter_pmi(key, record_group):
    cnt = 0
    for record in record_group:
        cnt += 1
        if cnt > 9:
            break
        yield {
            'doc_id': key,
            'word': record['word'],
        }


def build_pmi_graph():
    graph = Graph()
    input_stream = graph.input('text')
    splitted_words = input_stream.map(split_words)
    doc_word_count = splitted_words.\
        sort(keys=['doc_id']).\
        reduce(doc_unique_word_count, keys=['doc_id'])
    doc_word_freq = doc_word_count.map(comp_freq)
    filtered_words = doc_word_count.\
        sort(keys=['word']). \
        reduce(filter_rare, keys=['word']).\
        map(filter_short)

    total_word_count = splitted_words.fold(comp_total_word_count, {'total_word_count': 0})
    pmi_denominator = splitted_words.\
        sort(['word']).\
        reduce(comp_word_count, ['word']).\
        join(total_word_count).\
        map(comp_denominator)
    doc_word_freq.join(pmi_denominator, ['word'], strategy='inner').\
        join(filtered_words, keys=['word'], strategy='left').\
        map(comp_pmi).\
        sort(['doc_id', 'pmi']).\
        reduce(filter_pmi, ['doc_id']).output('load')

    graph.run()


def comp_root_length(record):
    R = 6378.137
    dLat = record['end'][0] * pi / 180 - record['start'][0] * pi / 180
    dLon = record['end'][1] * pi / 180 - record['start'][1] * pi / 180
    a = sin(dLat / 2) * sin(dLat / 2) + cos(record['start'][0] * pi / 180) * \
        cos(record['end'][0] * pi / 180) * sin(dLon / 2) * sin(dLon / 2)
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    d = R * c
    record['length_km'] = d
    yield record


def add_day_hour(record):
    enter_time = uci_parser.parse(record['enter_time'])
    leave_time = uci_parser.parse(record['leave_time'])
    time_delta = leave_time - enter_time

    record['time_delta_h'] = time_delta.total_seconds() / 3600
    record['weekday'] = enter_time.weekday()
    record['hour'] = enter_time.hour

    yield record


def comp_speed(record):
    record['speed'] = record['length_km'] / record['time_delta_h']

    yield record


def comp_avg_speed(key, record_group):
    total_speed = 0
    total_count = 0
    for record in record_group:
        total_speed += record['speed']
        total_count += 1

    yield {
        'weekday': calendar.day_name[key[0]],
        'hour': key[1],
        'speed': total_speed / total_count
    }


def build_yandex_maps_graph(roots_file_name, traffic_file_name):
    g = Graph()
    roots_stream = g.input('file').\
        map(comp_root_length)
    traffic_stream = g.input(file=traffic_file_name).map(add_day_hour).\
        join(roots_stream, ['edge_id'], 'left').\
        map(comp_speed).\
        sort(['weekday', 'hour']).\
        reduce(comp_avg_speed, ['weekday', 'hour']).\
        output('load')

    return g
