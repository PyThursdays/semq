# SEMQ

`SEMQ` stands for **Simple External Memory Queue**. The main motivation is to provide a simple, lightweight, python-based
implementation of an external queue that can be shared with different python processes concurrently.

Install via pip:

```commandline
$ TBD
```

## Development Installation Guide

Clone the repository and install via:

```commandline
$ pip install -e .
```

## Usage


```commandline
$ python -m semq setup --name <queue-name>
```
* The output should be the path to the metastore.
* You can modify the metastore target directory by:
  * Using the `--metastore_path` argument.
  * Defining the following env.var: `SEMQ_DEFAULT_METASTORE_PATH`

Example:

```commandline
$ python -m semq setup --name example
```
* Expected result: a new directory structure called `./metastore/example`

### `PUT` an element into the queue

**Via CLI App**

```commandline
$ python -m semq put --name example --item item-1
$ python -m semq put --name example --item item-2
$ python -m semq put --name example --item item-3
```

**Via Python**

```python
from semq import SimpleExternalQueue

# Create queue instance
queue = SimpleExternalQueue(name="example")

# Put items into the queue
queue.put(item="item-1")
queue.put(item="item-2")
queue.put(item="item-3")
```

### `GET` an element from the queue

**Via CLI App**

```commandline
$ python -m semq get --name example
```

**Via Python**

```python
from semq import SimpleExternalQueue

# Create queue instance
queue = SimpleExternalQueue(name="example")

# Get an item from the queue
item = queue.get()
print(item)
```

### `Size`: queue size

**Via CLI App**

```commandline
$ python -m semq size --name example
```

**Via Python**

```python
from semq import SimpleExternalQueue

# Create queue instance
queue = SimpleExternalQueue(name="example")

# Get the queue size
queue.size(
  include_items=True,
  ignore_requests=False,
)
```