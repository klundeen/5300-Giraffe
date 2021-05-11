# 5300-Instructor

#Spint Otoño
Ben Gruher & Adama Sanoh
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


## Sprint 2 -> 3 Handoff Video
https://user-images.githubusercontent.com/49925867/117762783-a12eff80-b1de-11eb-89dc-63ce82513275.mp4

