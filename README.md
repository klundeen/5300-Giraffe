# 5300-Giraffe

#Spint Invierno
Harsha Thulasi and Ryan Bush
First start by cloning the repository:
<pre>
git clone https://github.com/klundeen/5300-Giraffe
</pre>
## Mileston3: Schema Storage
These functions were created:
- <code>CREATE Table</code>
- <code>DROP Table</code>
- <code>SHOW Tables</code>
- <code>SHOW Columns</code>
All functions are working correctly.
## Mileston4: Schema Storage
These functions were created:
- <code>CREATE Index</code>
- <code>SHOW Index</code>
- <code>DROP IndexDr</code>
All functions are working with the exception of the show index and drop_table functions.
## Mileston5: Insert, Delete, Simple Queries
These functions were created:
- <code>INSERT INTO table [WHERE ...]</code>
- <code>DELETE FROM table [WHERE ...]</code>
- <code>SELECT [*|col1,col2...] FROM table [WHERE ...]</code>
The `WHERE` statements only support `AND` conjunction and `=` operator
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

## Unit Tests From Milestone2
There are some tests for SlottedPage and HeapTable from Milestone 2. After checking out the Milestone2 tag, they can be invoked from the <code>SQL</code> prompt:
```sql
SQL> test
```
Be aware that failed tests may leave garbage Berkeley DB files lingering in your data directory. If you don't care about any data in there, you are advised to just delete them all after a failed test.
```sh
$ rm -f ~/cpsc5300/data/*
```

## Valgrind (Linux)
To run valgrind (files must be compiled with <code>-ggdb</code>):
```sh
$ valgrind --leak-check=full --suppressions=valgrind.supp ./sql5300 ~/cpsc5300/data
```
Note that we've added suppression for the known issues with the Berkeley DB library <em>vis-à-vis</em> valgrind.

The valgrind has some memory leaks just from the inherited code from the Milestone5_prep:
```
5300-Giraffe$ rm -rf data/*
5300-Giraffe$ valgrind --leak-check=full --suppressions=valgrind.supp ./sql5300 data/
==610== Memcheck, a memory error detector
==610== Copyright (C) 2002-2017, and GNU GPL'd, by Julian Seward et al.
==610== Using Valgrind-3.13.0 and LibVEX; rerun with -h for copyright info
==610== Command: ./sql5300 data/
==610== 
(sql5300: running with database environment at data/)
SQL> create table foo (id int, data text)
CREATE TABLE foo (id INT, data TEXT)
created foo
SQL> show tables
SHOW TABLES
table_name 
+----------+
"foo" 
successfully returned 1 rows
SQL> show columns from foo
SHOW COLUMNS FROM foo
table_name column_name data_type 
+----------+----------+----------+
"foo" "id" "INT" 
"foo" "data" "TEXT" 
successfully returned 2 rows
SQL> create index fx on foo (id)
CREATE INDEX fx ON foo USING BTREE (id)
created index fx
SQL> create index fz on foo (data)
CREATE INDEX fz ON foo USING BTREE (data)
created index fz
SQL> show index from foo
SHOW INDEX FROM foo
table_name index_name column_name seq_in_index index_type is_unique 
+----------+----------+----------+----------+----------+----------+
"foo" "fx" "id" 1 "BTREE" true 
"foo" "fz" "data" 1 "BTREE" true 
successfully returned 2 rows
SQL> quit
==610== 
==610== HEAP SUMMARY:
==610==     in use at exit: 38,124 bytes in 105 blocks
==610==   total heap usage: 1,263 allocs, 1,158 frees, 232,145 bytes allocated
==610== 
==610== 72 (48 direct, 24 indirect) bytes in 2 blocks are definitely lost in loss record 52 of 92
==610==    at 0x4C3217F: operator new(unsigned long) (in /usr/lib/valgrind/vgpreload_memcheck-amd64-linux.so)
==610==    by 0x529C105: hsql_parse(hsql::SQLParserResult**, void*) (bison_parser.y:929)
==610==    by 0x5292EA1: hsql::SQLParser::parseSQLString(char const*) (SQLParser.cpp:29)
==610==    by 0x5292F29: hsql::SQLParser::parseSQLString(std::__cxx11::basic_string<char, std::char_traits<char>, std::allocator<char> > const&) (SQLParser.cpp:43)
==610==    by 0x10C90C: main (sql5300.cpp:51)
==610== 
==610== 224 (160 direct, 64 indirect) bytes in 2 blocks are definitely lost in loss record 69 of 92
==610==    at 0x4C3217F: operator new(unsigned long) (in /usr/lib/valgrind/vgpreload_memcheck-amd64-linux.so)
==610==    by 0x135E88: Indices::get_index(std::__cxx11::basic_string<char, std::char_traits<char>, std::allocator<char> >, std::__cxx11::basic_string<char, std::char_traits<char>, std::allocator<char> >) (schema_tables.cpp:407)
==610==    by 0x1244BE: SQLExec::create_index(hsql::CreateStatement const*) (SQLExec.cpp:356)
==610==    by 0x124F2C: SQLExec::create(hsql::CreateStatement const*) (SQLExec.cpp:270)
==610==    by 0x12AC7F: SQLExec::execute(hsql::SQLStatement const*) (SQLExec.cpp:72)
==610==    by 0x10CA5C: main (sql5300.cpp:60)
==610== 
==610== LEAK SUMMARY:
==610==    definitely lost: 208 bytes in 4 blocks
==610==    indirectly lost: 88 bytes in 6 blocks
==610==      possibly lost: 0 bytes in 0 blocks
==610==    still reachable: 21,402 bytes in 87 blocks
==610==                       of which reachable via heuristic:
==610==                         multipleinheritance: 328 bytes in 1 blocks
==610==         suppressed: 16,426 bytes in 8 blocks
==610== Reachable blocks (those to which a pointer was found) are not shown.
==610== To see them, rerun with: --leak-check=full --show-leak-kinds=all
==610== 
==610== For counts of detected and suppressed errors, rerun with: -v
==610== ERROR SUMMARY: 2 errors from 2 contexts (suppressed: 0 from 0)
5300-Giraffe$ 
```


## Sprint 2 -> 3 Handoff Video
https://user-images.githubusercontent.com/49925867/117762783-a12eff80-b1de-11eb-89dc-63ce82513275.mp4
