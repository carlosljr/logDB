# logDB
An implementation in Go of a log structured append-only with hash table indexing storage engine.

## 1. Basic Operations

logDB is a basic storage engine with two main operations. It is based in key/value storage where key will be indexed to retrieve its correspondent value.

- **get \<key\>**: To retrieve a value based in a given key.
- **set \<key\> \<value\>**: To store a given key/value and index key in a hash-table map.

There are automatic operations made for storage optimization. The **compaction** will remove redundant key indexations and keep the last index in a segment. The **merge** operation will get compacted segments and unite them in one new segment, also removing redundant keys between segments. These two operations are made in sequence between a pre-determined interval of 30 seconds by default. However, this interval is configurable as will be seen later.

## 2. Running Instructions

To execute logDB, use the pre-compiled binary in `release/ubuntu` for Linux Ubuntu distributions or compile and run the project using Go compiler installed in your machine.



### 2.1 Using the binary

The pre-compiled binary available is addressed for Ubuntu's distribution of Linux OS.

```
$ ./logDB <segmentSize> <compactAndMergeInterval>
```
where:
1. **segmentSize** (*Optional*) - An integer value used to define the maximum size of a segment in terms of number of lines (e.g. segmentSize = 5 will generate a segment up to 5 lines). When it reaches the size limit, a new segment will be generated to store keys and values. **Default value: 3**
2. **compactAndMergeInterval** (*Optional*) - An integer value to define the interval between compact/merge operations, in seconds. The generated segments will be compacted and merge between this interval. Current running segment won't be compacted or merged to avoid concurrency with write operations.
**Default value: 30**

### 2.2 Using Go tool
Compile and run logDB using the go tool.

```
$ go run main.go <segmentSize> <compactAndMergeInterval>
```

## 3. Usage Instructions

An example to store a new key/value in logDB. It will store `Michael` key and `Jackson` value:
```
Insert your command and press enter:

-> set Michael

<press Enter>

Insert the value for this key and press enter:

-> Jackson

Value "Jackson" stored with success!
```

When inserted a key/value, this data will be stored in a segment created in `log_storage` directory. This directory resides in your current path and it will keep all created segments. A segment is a file with `.log` extension and its prefix name 

```
logfile_<segment_number>
```
where:
segment_number: The sequence segment number. First segment created will have `1` value and so on.

A segment resulted from a merged operation will be

```
logfile-merged_<segment_number>
```

with `segment_number` following the same sequence.

After a key/value is stored, it will be appended in a new line of the last segment file. Key and value will be separated by a comma. In the example above, the stored is stored like bellow:

```
Michael,Jackson
```
in `logfile_1.log` file.

To get a value from a given key follows these instructions:

```
Insert your command and press enter:

-> get Michael

<Press enter>

Result:

-> Jackson

```

For exit logDB:

```
Insert your command and press enter:

-> exit

See ya!
```

After you exit from logDB, all stored data will be kept in `log_storage` directory. When you execute again, logDB will recover all data from its storage and rebuild the hash-table indexing for each existing segment. You don't need to worry about missing data! It's all safe!
