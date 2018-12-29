import os
from lib import Graph


SCRIPT_PATH = os.path.realpath(os.path.dirname(__file__))


def naive_mapper(row: dict):
    yield row


def test_graph_naive_map():
    graph = Graph()
    graph.input('filename').map(naive_mapper).output(output_type='load')
    graph_output = graph.run(filename=os.path.join(SCRIPT_PATH, 'test_resources', 'valid_input.txt'))
    valid_output = [{"one": 1, "two": 2, "tree": "node"}, {"first": 1, "second": 2}]
    assert graph_output == valid_output


def test_graph_sort_and_map():
    graph = Graph()
    graph.input('filename').sort(keys=["age"]).map(naive_mapper).output(output_type='load')
    graph_output = graph.run(filename=os.path.join(SCRIPT_PATH, 'test_resources', 'sort_and_map.txt'))
    valid_output = [{"name": "Ivan", "age": 2}, {"name": "Andrey", "age": 10}]
    assert graph_output == valid_output


def test_double_sort():
    graph = Graph()
    graph.input('filename').sort(keys=["name"]).sort(keys=["age"]).output(output_type='load')
    graph_output = graph.run(filename=os.path.join(SCRIPT_PATH, 'test_resources', 'sort_and_map.txt'))
    valid_output = [{"name": "Ivan", "age": 2}, {"name": "Andrey", "age": 10}]
    assert graph_output == valid_output


def word_cnt_mapper(row: dict):
    for word in row['text'].split():
        yield dict(word=word, cnt=1)


def word_cnt_reducer(key, record_group):
    yield {'word': key, 'total': sum(record['cnt'] for record in record_group)}


def test_word_count():
    graph = Graph()
    graph.input('filename').\
        map(word_cnt_mapper).\
        sort(keys=['word']).\
        reduce(word_cnt_reducer, keys=['word']).\
        output(output_type='load')

    graph_output = graph.run(filename=os.path.join(SCRIPT_PATH, 'test_resources', 'word_cnt_small.txt'))

    valid_reducer_output = [{'word': 'The', 'total': 1}, {'word': 'first', 'total': 1}, {'word': 'test', 'total': 2},
                            {'word': 'same', 'total': 1}, {'word': 'of', 'total': 1}, {'word': 'counting', 'total': 2},
                            {'word': 'words', 'total': 3}]

    assert graph_output == sorted(valid_reducer_output, key=lambda x: x['word'])


def sum_folder(state: dict, row_group: dict):
    new_state = {}
    new_state['number'] = state['number']
    for row in row_group:
        new_state['number'] += row['number']
    new_state['doc_id'] = state['doc_id']
    return new_state


def test_fold():
    graph = Graph()
    init_state = {'number': 0, 'doc_id': 1}
    graph.input('table').fold(sum_folder, init_state, keys=['doc_id']).output(output_type='load')

    table_to_fold = [
        {"doc_id": 1, "number": 1},
        {"doc_id": 2, "number": 2},
        {"doc_id": 3, "number": 1},
        {"doc_id": 3, "number": 1},
        {"doc_id": 3, "number": 1},
        {"doc_id": 3, "number": 1},
        {"doc_id": 4, "number": 4},
        {"doc_id": 5, "number": 5},
        {"doc_id": 6, "number": 6},
        {"doc_id": 7, "number": 7},
        {"doc_id": 8, "number": 8},
        {"doc_id": 9, "number": 1},
        {"doc_id": 9, "number": 9}
    ]

    graph_output = graph.run(table=table_to_fold)
    valid_output = [{"doc_id": 1, "number": 47}]
    assert graph_output == valid_output


def test_sort_one_field():
    graph = Graph()
    graph.input('filename').sort(keys=['item_price']).output(output_type='load')

    graph_output = graph.run(filename=os.path.join(SCRIPT_PATH, 'test_resources', 'sort.txt'))
    valid_output = [
        {"item_name": "pasta", "item_cnt": 0, "item_price": 10},
        {"item_name": "tomato paste", "item_cnt": 10, "item_price": 50},
        {"item_name": "toothpaste", "item_cnt": 1010, "item_price": 200},
        {"item_name": "toothbrush", "item_cnt": 1020, "item_price": 200},
        {"item_name": "paste", "item_cnt": 2000, "item_price": 500}
    ]
    assert graph_output == valid_output


def test_sort_two_fields():
    graph = Graph()
    graph.input('filename').sort(keys=['item_price', 'item_cnt']).output(output_type='load')

    graph_output = graph.run(filename=os.path.join(SCRIPT_PATH, 'test_resources', 'sort.txt'))
    valid_output = [
        {"item_name": "pasta", "item_cnt": 0, "item_price": 10},
        {"item_name": "tomato paste", "item_cnt": 10, "item_price": 50},
        {"item_name": "toothpaste", "item_cnt": 1010, "item_price": 200},
        {"item_name": "toothbrush", "item_cnt": 1020, "item_price": 200},
        {"item_name": "paste", "item_cnt": 2000, "item_price": 500}
    ]
    assert graph_output == valid_output


def test_split():
    graph_A = Graph()
    stream = graph_A.input('filename')
    stream.sort(keys=['BookID']).output(output_type='load')
    stream.sort(keys=['BookName']).output(output_type='load')

    output_1, output_2 = graph_A.run(filename=os.path.join(SCRIPT_PATH, 'test_resources', 'table_A.txt'))
    valid_output_1 = [
        {"BookID": 1, "BookName": "Thinking in Java"},
        {"BookID": 3, "BookName": "Modern Operating System"},
        {"BookID": 3, "BookName": "Computer Architecture"},
        {"BookID": 4, "BookName": "Programming in Scala"}
    ]
    assert output_1 == valid_output_1

    valid_output_2 = [
        {"BookID": 3, "BookName": "Computer Architecture"},
        {"BookID": 3, "BookName": "Modern Operating System"},
        {"BookID": 4, "BookName": "Programming in Scala"},
        {"BookID": 1, "BookName": "Thinking in Java"}
    ]
    assert output_2 == valid_output_2


def test_inner_join():
    graph = Graph()
    stream_A = graph.input('first_input').sort(keys=["BookID"])
    stream_B = graph.input('second_input').\
        sort(keys=["AuthorID"]).\
        join(stream_A, keys=('AuthorID', 'BookID'), strategy='inner').\
        sort(keys=["BookName"]).\
        output(output_type='load')

    graph_output = graph.run(first_input=os.path.join(SCRIPT_PATH, 'test_resources', 'table_A.txt'),
                             second_input=os.path.join(SCRIPT_PATH, 'test_resources', 'table_B.txt'))
    valid_output = [
        {"AuthorID": 3, "AuthorName": "Andrew Tanenbaum", "BookID": 3, "BookName": "Computer Architecture"},
        {"AuthorID": 3, "AuthorName": "Andrew Tanenbaum", "BookID": 3, "BookName": "Modern Operating System"},
        {"AuthorID": 1, "AuthorName": "Bruce Eckel", "BookID": 1, "BookName": "Thinking in Java"}
    ]

    assert graph_output == valid_output


def test_outer_join():
    graph = Graph()
    stream_A = graph.input('first_input').sort(keys=["BookID"])
    stream_B = graph.input('second_input').\
        join(stream_A, keys=('AuthorID', 'BookID'), strategy='outer').\
        output(output_type='load')

    graph_output = graph.run(first_input=os.path.join(SCRIPT_PATH, 'test_resources', 'table_A.txt'),
                             second_input=os.path.join(SCRIPT_PATH, 'test_resources', 'table_B.txt'))
    valid_output = [
        {"AuthorID": 1, "AuthorName": "Bruce Eckel", "BookID": 1, "BookName": "Thinking in Java"},
        {"AuthorID": 2, "AuthorName": "Robert Lafore"},
        {"AuthorID": 3, "AuthorName": "Andrew Tanenbaum", "BookID": 3, "BookName": "Modern Operating System"},
        {"AuthorID": 3, "AuthorName": "Andrew Tanenbaum", "BookID": 3, "BookName": "Computer Architecture"},
        {"BookID": 4, "BookName": "Programming in Scala"},
    ]

    assert graph_output == valid_output


def test_left_join():
    graph = Graph()
    stream_A = graph.input('first_input').sort(keys=["BookID"])
    stream_B = graph.input('second_input').\
        join(stream_A, keys=('AuthorID', 'BookID'), strategy='left').\
        output(output_type='load')

    graph_output = graph.run(first_input=os.path.join(SCRIPT_PATH, 'test_resources', 'table_A.txt'),
                             second_input=os.path.join(SCRIPT_PATH, 'test_resources', 'table_B.txt'))
    valid_output = [
        {"AuthorID": 1, "AuthorName": "Bruce Eckel", "BookID": 1, "BookName": "Thinking in Java"},
        {"AuthorID": 2, "AuthorName": "Robert Lafore"},
        {"AuthorID": 3, "AuthorName": "Andrew Tanenbaum", "BookID": 3, "BookName": "Modern Operating System"},
        {"AuthorID": 3, "AuthorName": "Andrew Tanenbaum", "BookID": 3, "BookName": "Computer Architecture"},
    ]
    assert graph_output == valid_output


def test_right_join():
    graph = Graph()
    stream_A = graph.input('first_input').sort(keys=["BookID"])
    stream_B = graph.input('second_input').\
        join(stream_A, keys=('AuthorID', 'BookID'), strategy='right').\
        output(output_type='load')

    graph_output = graph.run(first_input=os.path.join(SCRIPT_PATH, 'test_resources', 'table_A.txt'),
                             second_input=os.path.join(SCRIPT_PATH, 'test_resources', 'table_B.txt'))

    valid_output = [
        {"AuthorID": 1, "AuthorName": "Bruce Eckel", "BookID": 1, "BookName": "Thinking in Java"},
        {"AuthorID": 3, "AuthorName": "Andrew Tanenbaum", "BookID": 3, "BookName": "Modern Operating System"},
        {"AuthorID": 3, "AuthorName": "Andrew Tanenbaum", "BookID": 3, "BookName": "Computer Architecture"},
        {"BookID": 4, "BookName": "Programming in Scala"},
        # {"AuthorID": None, "AuthorName": None, "BookID": 4, "BookName": "Programming in Scala"},
    ]
    assert graph_output == valid_output


def test_join_by_empty_keys():
    graph = Graph()
    stream_A = graph.input('first_input')
    stream_B = graph.input('second_input').\
        join(stream_A, strategy='outer').\
        output(output_type='load')

    graph_output = graph.run(first_input=os.path.join(SCRIPT_PATH, 'test_resources', 'table_A.txt'),
                             second_input=os.path.join(SCRIPT_PATH, 'test_resources', 'table_B.txt'))
    valid_output = [
        {'AuthorID': 1, 'AuthorName': 'Bruce Eckel', 'BookID': 3, 'BookName': 'Modern Operating System'},
        {'AuthorID': 1, 'AuthorName': 'Bruce Eckel', 'BookID': 1, 'BookName': 'Thinking in Java'},
        {'AuthorID': 1, 'AuthorName': 'Bruce Eckel', 'BookID': 3, 'BookName': 'Computer Architecture'},
        {'AuthorID': 1, 'AuthorName': 'Bruce Eckel', 'BookID': 4, 'BookName': 'Programming in Scala'},
        {'AuthorID': 2, 'AuthorName': 'Robert Lafore', 'BookID': 3, 'BookName': 'Modern Operating System'},
        {'AuthorID': 2, 'AuthorName': 'Robert Lafore', 'BookID': 1, 'BookName': 'Thinking in Java'},
        {'AuthorID': 2, 'AuthorName': 'Robert Lafore', 'BookID': 3, 'BookName': 'Computer Architecture'},
        {'AuthorID': 2, 'AuthorName': 'Robert Lafore', 'BookID': 4, 'BookName': 'Programming in Scala'},
        {'AuthorID': 3, 'AuthorName': 'Andrew Tanenbaum', 'BookID': 3, 'BookName': 'Modern Operating System'},
        {'AuthorID': 3, 'AuthorName': 'Andrew Tanenbaum', 'BookID': 1, 'BookName': 'Thinking in Java'},
        {'AuthorID': 3, 'AuthorName': 'Andrew Tanenbaum', 'BookID': 3, 'BookName': 'Computer Architecture'},
        {'AuthorID': 3, 'AuthorName': 'Andrew Tanenbaum', 'BookID': 4, 'BookName': 'Programming in Scala'}
    ]
    assert graph_output == valid_output


def test_rhombus_join():
    graph = Graph()
    stream = graph.input('input_name')
    stream_2 = stream.sort(['BookID'])
    stream = stream.sort(['BookName'])
    stream.join(stream_2, ['BookID'], 'inner').output('print')

    graph.run(input_name=os.path.join(SCRIPT_PATH, 'test_resources', 'table_A.txt'))


def test_join_with_overlap():
    graph = Graph()
    stream_A = graph.input('first_input')
    stream_B = graph.input('second_input'). \
        join(stream_A, keys=['id'], strategy='inner', lsuffix='_1', rsuffix='_2'). \
        output(output_type='load')

    left_table = [
        {'id': 1, 'name': 'Рыбка Поньо на утёсе'},
        {'id': 2, 'name': '7 самураев'},
        {'id': 3, 'name': 'Расёмон'},
        {'id': 4, 'name': 'Old boy'},
    ]

    right_table = [
        {'id': 1, 'name': 'Хаяо Миядзаки'},
        {'id': 2, 'name': 'Акира Куросава'},
        {'id': 3, 'name': 'Акира Куросава'},
        {'id': 4, 'name': 'Пак Чхан Ук'},
    ]

    graph_output = graph.run(first_input=right_table,
                             second_input=left_table)

    valid_output = [
        {'id_1': 1, 'name_1': 'Рыбка Поньо на утёсе', 'id_2': 1, 'name_2': 'Хаяо Миядзаки'},
        {'id_1': 2, 'name_1': '7 самураев', 'id_2': 2, 'name_2': 'Акира Куросава'},
        {'id_1': 3, 'name_1': 'Расёмон', 'id_2': 3, 'name_2': 'Акира Куросава'},
        {'id_1': 4, 'name_1': 'Old boy', 'id_2': 4, 'name_2': 'Пак Чхан Ук'},
    ]

    assert graph_output == valid_output


if __name__ == "__main__":
    test_rhombus_join()
