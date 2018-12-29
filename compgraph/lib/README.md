# ComputeGraph Library


### Description
This library can be used to solve tasks with computational graph.

#### What is a computational graph?
A computational graph is a directed graph where the nodes correspond to operations. All the operations are perfomed on tables (format of their representation is described below).

#### What operations it can compute?
1. **Map** performs transformation of each row of table with a function called mapper. Mapper takes each row and should return several (maybe 0) rows. 
2. **Sort** performs sorting of whole table with respect to values in *key* columns (parameter *key* is passed to Sort). 
3. **Fold** combines all rows of table with a function called folder. Each row is combined with state and new state is returned (for more details read [article on Wikipedia](https://en.wikipedia.org/wiki/Fold_(higher-order_function)))
4. **Reduce** performs transformation of groups of rows (grouped by key value) with a function called reducer. Reducer takes the key and the group of records and return several (maybe 0) rows. **Sort should be performed before any Reduce.** 
5. **Join** combines columns from table A with columns from table B on values from keys columns. Different strategies of joining are implemented: inner join, outer join, left join, right join (have same behaviour as in SQL; for futher details read [article on Wikipedia](https://en.wikipedia.org/wiki/Join_(SQL)))

### Package installation

1. Clone the repository to your local compute
2. Change your directory on the directory where files of the library were cloned
3. In command line run
    ```console
    pip install .
    ```
4. Engoy the package! Now in your programs you can simply import new package with ordinary statement:
```python
from graph import Graph
```

### Package uninstallation
If, unfortunatelly, you want to uninstall package run in command line:
    ```console
    pip uninstall graph
    ```
### Usage
Firstly all operations have to be described and added to graph and only after that graph can be executed.

#### Graph specification
1. Declare graph
    ```python
    g = Graph()
    ```
2. Add a new input node.
    ```python
    stream = g.input("file_name")
    ```
   Input function will return Stream object, which has methods to add other nodes in graph. 
   Possible nodes to add: 
   - map(mapper)
   - sort(keys)
   - fold(folder, init_state) 
   - reduce(reducer, keys)
   - join(another_stream, keys, strategy)
3. If you want to produce output add output node:
    ```python
    stream.output(type)
    ```
   With *type* param you can choose what kind of output is needed (possible options: *'print'* - prints all outputs merged, *'load'* - returns outputs as list of lists, *'save'* - saves all outputs to file).
#### Input format
*In this version of a graph input can read only from file.*

File should contain dicts each representing one row of table. The file should be saved in json format.

#### Run graph
After graph configuration is set the graph can be executed with a command ```g.run()```.

**Note**: stream objects are used only to configurate graph, but not to run it.

### Examples
1. Linear graph that splits strings from 'text' column on separate words:
    ```python
    from graph import Graph
    
    g = Graph()
    g.input("data").map(splitter).output('load')
    
    g.run()
    ```
    Possible implementation of splitter:
    ```python
    def splitter(row):
        for word in row['text'].split():
            yield {
                'word': ''.join(
                    filter(lambda c: c.isalpha(), word.lower())
                    )
            }
    ```

2. Graph that counts number of words in test corpus and filter long words (example is quite useless in life but shows how not linear graphs could be specified):
    ```python
    from graph import Graph
    
    g = Graph()
    splitted_words = g.input("data").map(splitter)
    filter_short = splitted_words.map(filter_short)
    count_words = splitted_words.\
                fold(folder, {'total_word_count': 0}).\
                join(filter_short)
    
    g.run()
    ```
    **Note:** Join with two keys will do one of strategies ('inner' by default). Join with one key will try to find key column both in table_A and in table_B. Join without keys will perform cross join (decart product of tables rows).

3. For more examples of package usage check [this file](../algorithms.py).

### Tests
All unit tests are in *tests* folder.

### TO DO in version 1.1
1. Add other ways to pass input to graph (from a stream, for example).
2. Make Reduce check if input is sorted.
3. Make Join more efficient.