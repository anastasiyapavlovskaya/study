import json
import logging
import types
import typing
from abc import abstractmethod
from itertools import groupby, product
from operator import itemgetter

logging.basicConfig(format='%(message)s', level=logging.DEBUG)
logger = logging.getLogger(__name__)


class Node:
    """
    Node is the base object for any operation. For operation description read inheritors docstrings.
    All operations deal with iterable seqence of dicts.
    """
    def __init__(self, graph):
        self._graph = graph
        self._id = len(graph._nodes)

        self._parent = None
        self._left_parent = None
        self._right_parent = None

        self._children_cnt = 0
        self._not_calculated_children_cnt = 0

    def _run_parent(self, parent_node: 'Node') -> None:
        """
        Computes parent_node or takes already computed values.
        :param parent_node: is node to be computed
        :return:
        """
        is_not_calculated = parent_node._id not in parent_node._graph._nodes_outputs.keys()

        if is_not_calculated and parent_node._children_cnt == 1:
            parent_node._graph._nodes_outputs[parent_node._id] = parent_node._run()
        elif is_not_calculated and parent_node._children_cnt > 1:
            parent_node._graph._nodes_outputs[parent_node._id] = list(parent_node._run())

        parent_node._not_calculated_children_cnt -= 1

    def _clean_memory(self, parent_node: 'Node') -> None:
        """
        Delete output of nodes, which won't be used for computing of other nodes.
        :param parent_node: is a candidate to be deleted
        :return:
        """
        logger.debug('CLEANING {}'.format(str(self)))
        if parent_node._not_calculated_children_cnt == 0:
            try:
                del parent_node._graph._nodes_outputs[parent_node._id]
            except KeyError:
                pass

    def _run(self):
        """
        Call execution of node_parents and clean memory after node is calculated.
        :return:
        """
        logger.debug("IN NODE :{} - running parent".format(str(self)))
        if self._parent:
            self._run_parent(self._parent)
        elif self._left_parent and self._right_parent:
            self._run_parent(self._left_parent)
            self._run_parent(self._right_parent)

        logger.debug("IN NODE: {} - running".format(str(self)))
        returned_val = None
        if isinstance(self, Output):
            returned_val = self.run()
        else:
            yield from self.run()

        logger.debug("IN NODE: {} - cleaning".format(str(self)))
        if self._parent:
            self._clean_memory(self._parent)

        elif self._left_parent and self._right_parent:
            self._clean_memory(self._left_parent)
            self._clean_memory(self._right_parent)

        if returned_val:
            return returned_val

    @abstractmethod
    def run(self):
        """
        Execute operation.
        :return:
        """
        pass

    def __repr__(self) -> str:
        return "{} - {}Node".format(self._id, self.__class__.__name__)


class Stream:
    """
    Class for graph specification. Stream has reference to a graph and nodes could be added to the graph
    only with a help of Stream. Each method returns Stream thus other nodes can be added.
    """
    def __init__(self, graph, node):
        self._graph = graph
        self._node = node

    def map(self, mapper: types.GeneratorType) -> 'Stream':
        """
        Adds map node to a graph specified with Stream.
        :param mapper: function witch takes iterable and returns iterable
        :return: Stream with reference to graph with new node
        """
        return Stream(
            self._graph,
            self._graph._add_node(Map, parent_node=self._node, mapper=mapper)
        )

    def sort(self, keys: list) -> 'Stream':
        """
        Adds sort node to a graph specified with Stream.
        :param keys: names of columns to sort in sorting order
        :return: Stream with reference to graph with new node
        """
        return Stream(
            self._graph,
            self._graph._add_node(Sort, self._node, keys=keys)
        )

    def fold(self, folder: types.FunctionType, init_state: dict, keys: list) -> 'Stream':
        """
        Adds fold node to a graph specified with Stream.
        :param folder: function that takes new record and state as an input and returns state
        :param init_state: state that will be changed with other records
        :param keys: list of keys to group records
        :return: Stream with reference to graph with new node
        """
        return Stream(
            self._graph,
            self._graph._add_node(Fold, self._node, folder=folder, init_state=init_state, keys=keys)
        )

    def reduce(self, reducer: types.GeneratorType, keys: list) -> 'Stream':
        """
        Adds reduce node to a graph specified with Stream.
        :param reducer: function witch takes group of iterables and returns iterable
        :param keys: names of columns for grouping
        :return: Stream with reference to graph with new node
        """
        return Stream(
            self._graph,
            self._graph._add_node(Reduce, self._node, reducer=reducer, keys=keys)
        )

    def join(self, another_stream, keys=list(), strategy='inner', lsuffix='', rsuffix='') -> 'Stream':
        """
        :param another_stream:
        :param keys: list of column names on which the join should execute
        :param strategy: 'inner', 'outer', 'right', 'left' - possible types of join strategies.
        Have the same behaviour as in SQL.
        :param lsuffix: suffix, that will be added to overlapping columns of left table
        :param rsuffix: suffix, that will be added to overlapping columns of right table
        :return: Stream with reference to graph with new node
        """
        return Stream(
            self._graph,
            self._graph._add_node(Join,
                                  left_node=self._node, right_node=another_stream._node,
                                  keys=keys, strategy=strategy, lsuffix=lsuffix, rsuffix=rsuffix)
        )

    def output(self, output_type: str, file_name=None) -> 'Stream':
        """
        Adds output node to a graph specified with Stream.
        :param output_type: 'print', 'load' or 'save' how the output should be returned
        :param file_name: name of file to save output; if type != 'save' this param will be ignored
        :return: Stream with reference to graph with new node
        """
        return Stream(
            self._graph,
            self._graph._add_node(Output, self._node, output_type=output_type, file_name=file_name)
        )


class Graph:
    def __init__(self):
        self._nodes = list()
        self._nodes_outputs = dict()
        self._input_name = None

    def print_config(self) -> None:
        """
        Print all nodes in the order they present in graph._nodes list.
        :return:
        """
        for node in self._nodes:
            print(str(node), end=' ')

    def input(self, name: str) -> Stream:
        """
        Creates Stream object and adds node with input. Have to be the first node of any graph.
        :param name: name of the argument to pass files and streams to graph.
        For example:
            graph = Graph().input(file_name).output('load')
            graph.run(file_name="")
        :return: Stream with reference to this graph.
        """
        return Stream(self, self._add_node(Input, name=name))

    def run(self, **kwargs) -> typing.Union[list, None]:
        """
        Computes graph.
        :param kwargs: names of inputs
        :return: if output type is load, then the result of calculation is returned. Otherwise, None is returned.
        """
        logger.debug('started to run')
        logger.debug(' '.join(str(node) for node in self._nodes))

        for node in self._nodes:
            if isinstance(node, Input):
                try:
                    node._resource = kwargs[node._name]
                except KeyError:
                    raise KeyError('Not all inputs are specified')

        for node in self._nodes:
            node._not_calculated_children_cnt = node._children_cnt

        returned_val = []
        for node in self._nodes:
            if isinstance(node, Output):
                try:
                    next(node._run())
                except StopIteration as to_return:
                    returned_val.append(to_return.value)
        if returned_val:
            if len(returned_val) == 1:
                return returned_val[0]
            else:
                return returned_val

    def _add_node(self, node_type, parent_node=None, **kwargs) -> Node:
        """
        Adds all types of nodes to graph.
        :param node_type: Input, Output, Map, Fold, Join, Reduce
        :param parent_node: node from which this node will take data
        :param kwargs: other needed arguments to construct node
        :return:
        """
        new_node = node_type(self, **kwargs)

        if node_type == Join:
            kwargs['left_node']._children_cnt += 1
            kwargs['right_node']._children_cnt += 1

        elif not node_type == Input:
            parent_node._children_cnt += 1
            new_node._parent = parent_node

        self._nodes.append(new_node)
        return new_node


class Input(Node):
    """
    Provides input to graph. In first version of library input could be provided only from a file.
    A file should contain sequence of dict and should be saved in json format.
    """
    def __init__(self, graph, name: str):
        super().__init__(graph)
        self._name = name
        self._resource = None

    def run(self) -> None:
        """
        Execute operation.
        :return:
        """
        if isinstance(self._resource, str):
            with open(self._resource, 'r') as input_file:
                for row in input_file:
                    yield json.loads(row)
        else:
            try:
                for row in self._resource:
                    yield row
            except TypeError:
                raise Exception('Not supported type of input')


class Map(Node):
    """
    The mapper takes a series of key/value pairs, processes each, and generates zero or more output key/value pairs.
    """
    def __init__(self, graph: Graph, mapper: types.GeneratorType):
        super().__init__(graph)
        self._mapper = mapper

    def run(self) -> None:
        """
        Execute operation.
        :return:
        """
        for record in self._graph._nodes_outputs[self._parent._id]:
            yield from self._mapper(record)


class Sort(Node):
    """
    Sorts the whole table by the keys.
    """
    def __init__(self, graph, keys):
        super().__init__(graph)
        self._keys = keys

    def run(self) -> None:
        """
        Execute operation.
        :return:
        """

        table_to_sort = []
        for record in self._graph._nodes_outputs[self._parent._id]:
            table_to_sort.append(record)

        table_to_sort = sorted(table_to_sort, key=itemgetter(*self._keys))
        for record in table_to_sort:
            yield record


class Fold(Node):
    """
    The light version of Reduce: doesn't need data to be sorted.
    """
    def __init__(self, graph, keys: list, folder: types.FunctionType, init_state: dict):
        super().__init__(graph)
        self._folder = folder
        self._state = init_state
        self._keys = keys

    def run(self) -> None:
        """
        Execute operation.
        :return:
        """
        folded_groups = []
        for key, group in groupby(self._graph._nodes_outputs[self._parent._id],
                                  key=itemgetter(*self._keys)):
            folded_groups.append(self._folder(self._state, group))

        folded_groups = sorted(folded_groups, key=itemgetter(*self._keys))
        yield self._folder(self._state, folded_groups)


class Reduce(Node):
    """
    Calls reducer function once for each unique key in the sorted order.
    The reducer can iterate through the values that are associated with that key and produce zero or more outputs.
    """
    def __init__(self, graph, reducer: types.GeneratorType, keys: list):
        super().__init__(graph)
        self._reducer = reducer
        self._keys = keys

    def run(self) -> None:
        """
        Execute operation.
        :return:
        """
        prev_key = None
        for key, group in groupby(self._graph._nodes_outputs[self._parent._id],
                                  key=itemgetter(*self._keys)):
            if prev_key and key < prev_key:
                raise Exception('Data is not sorted')
            yield from self._reducer(key, group)
            prev_key = key


class Join(Node):
    """
    Combines columns from one or more tables by using values common to each.
    INNER_JOIN: An inner join requires each row in the two joined tables to have matching column values. I
    nner join creates a new result table by combining column values of two tables (A and B) based upon the matching of
    two columns values.
    LEFT_JOIN: The result of a left join for tables A and B always contains all rows of the "left" table (A), even if
    the join-condition does not find any matching row in the "right" table (B).
    RIGHT_JOIN: A right join closely resembles a left outer join, except with the treatment of the tables reversed.
    Every row from the "right" table (B) will appear in the joined table at least once. If no matching row from the
    "left" table (A) exists, NULL will appear in columns from A for those rows that have no match in B.
    OUTER_JOIN: Combines the effect of applying both left and right joins. Where rows in the OUTER JOINed tables do not
    match, the result set will have NULL values for every column of the table that lacks a matching row. For those rows
    that do match, a single row will be produced in the result set (containing columns populated from both tables).

    Join with two keys will do one of this strategies ('inner' by default). Join with one key will try to find key
    column both in table_A and in table_B. Join without keys will perform cross join (decart product of tables rows).
    """
    def __init__(self, graph: Graph, keys=list(), strategy='outer',
                 left_node=None, right_node=None,
                 lsuffix='left', rsuffix='right'):
        super().__init__(graph)
        self._keys = keys
        self._strategy = strategy
        self._left_parent = left_node
        self._right_parent = right_node
        self._lsuffix = lsuffix
        self._rsuffix = rsuffix

    def run(self) -> None:
        """
        Execute operation.
        :return:
        """
        right_table = self._right_parent._graph._nodes_outputs[self._right_parent._id]
        left_table = self._left_parent._graph._nodes_outputs[self._left_parent._id]

        if len(self._keys) == 0:
            for left_row, right_row in product(left_table, right_table):
                yield {**left_row, **right_row}
            return

        if len(self._keys) == 1:
            left_key_to_join = right_key_to_join = itemgetter(self._keys[0])

        if len(self._keys) == 2:
            left_key_to_join = itemgetter(self._keys[0])
            right_key_to_join = itemgetter(self._keys[1])

        right_table_grouped = groupby(right_table, key=right_key_to_join)
        right_key, right_group = next(right_table_grouped)

        left_table_grouped = groupby(left_table, key=left_key_to_join)
        for left_key, left_group in left_table_grouped:
            while left_key > right_key:
                if self._strategy == 'right' or self._strategy == 'outer':
                    for row in right_group:
                        yield row
                try:
                    right_key, right_group = next(right_table_grouped)
                except StopIteration:
                    break

            if left_key < right_key:
                if self._strategy == 'left' or self._strategy == 'outer':
                    for row in left_group:
                        yield row
                continue

            if left_key == right_key:
                for left_row, right_row in product(left_group, right_group):
                    columns_overlap = set(left_row.keys()).intersection(set(right_row.keys()))
                    if not columns_overlap:
                        yield {**left_row, **right_row}
                    else:
                        new_left_keys = [key + self._lsuffix if key in columns_overlap else key
                                         for key in left_row.keys()]
                        new_right_keys = [key + self._rsuffix if key in columns_overlap else key
                                          for key in right_row.keys()]

                        yield dict(zip(new_left_keys + new_right_keys,
                                       list(left_row.values()) + list(right_row.values())))

        if self._strategy == 'right' or self._strategy == 'outer':
            for right_key, right_group in right_table_grouped:
                for row in right_group:
                    yield row

        if self._strategy == 'left' or self._strategy == 'outer':
            for left_key, left_group in left_table_grouped:
                for row in left_group:
                    yield row


class Output(Node):
    """
    Gives the output for the graph.
    Possible types of output are
        - 'print' – prints output;
        - 'load' – returns output from run;
        - 'save' – opens file with file_name and writes output there
    """
    def __init__(self, graph, output_type="print", file_name=None):
        super().__init__(graph)
        self._type = output_type
        self.file_name = file_name
        self.verbose = False

    def run(self) -> typing.Union[list, None]:
        """
        Execute operation.
        :return:
        """

        if self._type == "print":
            for record in self._graph._nodes_outputs[self._parent._id]:
                print(record)

        elif self._type == "save":
            try:
                with open(self.file_name, "w") as output_file:
                    for record in self._graph._nodes_outputs[self._parent._id]:
                        output_file.write(record)
            except FileExistsError or FileNotFoundError as e:
                logger.error(e)
                raise

        elif self._type == "load":
            result = []
            for record in self._graph._nodes_outputs[self._parent._id]:
                result.append(record)

            return result
