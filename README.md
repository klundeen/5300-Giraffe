# 5300-Giraffe

#Spint Invierno
Harsha Thulasi and Ryan Bush
First start by cloning the repository:
<pre>
git clone https://github.com/klundeen/5300-Giraffe
</pre>
## Milestone 3: Schema Storage
These functions were created:
- <code>CREATE Table</code>
- <code>DROP Table</code>
- <code>SHOW Tables</code>
- <code>SHOW Columns</code>
All functions are working correctly.
## Milestone 4: Schema Storage
These functions were created:
- <code>CREATE Index</code>
- <code>SHOW Index</code>
- <code>DROP IndexDr</code>
All functions are working with the exception of the show index and drop_table functions.
## Milestone 5: Insert, Delete, Simple Queries
These functions were created:
- <code>INSERT INTO table [WHERE ...]</code>
- <code>DELETE FROM table [WHERE ...]</code>
- <code>SELECT [*|col1,col2...] FROM table [WHERE ...]</code>
The `WHERE` statements only support `AND` conjunction and `=` operator
## Milestone 6: insert and lookup
These functions were created:
- <code>Insert for test in test_btree()</code>
- <code>Lookup for test in test_btree()</code>

These functions are not supported:
- delete
- range
## To run the program
<pre>
$ ./sql5300 ~/cpsc5300/data
</pre>

## Tags
- <code>Milestone1</code> is playing around with the AST returned by the HyLine parser and general setup of the command loop.
- <code>Milestone2h</code> has the intructor-provided files for Milestone2. (Note that heap_storage.cpp is just a stub.)
- <code>Milestone2</code> is the instructor's attempt to complete the Milestone 2 assignment.
- <code>Milestone3_prep</code> has the instructor-provided files for Milestone 3. The students' work is in <code>SQLExec.cpp</code> labeled with <code>FIXME</code>.
- <code>Milestone4</code> contains the final solution from the Otono group for Milestone 4.
- <code>Milestone5</code> contains the code milestone4 required tasks.
- <code>Milestone6_prep</code> contains the code merged from 5300-Instructor "Milestone6_prep" tag.
- <code>Milestone6</code> contains working code that does the insert and lookup operations via BTree. Does not yet support BTree index range and delete.

## Unit Tests From Milestone2
There are some tests for SlottedPage and HeapTable from Milestone 2. After checking out the Milestone2 tag, they can be invoked from the <code>SQL</code> prompt:
```sql
SQL> test
```
Be aware that failed tests may leave garbage Berkeley DB files lingering in your data directory. If you don't care about any data in there, you are advised to just delete them all after a failed test.
```sh
$ rm -rf ~/cpsc5300/data/*
```

## Valgrind (Linux)
To run valgrind (files must be compiled with <code>-ggdb</code>):
```sh
$ valgrind --leak-check=full --suppressions=valgrind.supp ./sql5300 ~/cpsc5300/data
```
Note that we've added suppression for the known issues with the Berkeley DB library <em>vis-à-vis</em> valgrind.

The valgrind has some memory leaks just from the inherited code from the Milestone5_prep and Milestone6_prep. Beyond that should not have any additional from Milestone6 implementation.


## Sprint 2 -> 3 Handoff Video
https://user-images.githubusercontent.com/49925867/117762783-a12eff80-b1de-11eb-89dc-63ce82513275.mp4
