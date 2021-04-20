# 5300-Giraffe
## Sprint 1 - Cheng and Bruno

## Milestone 1 Status: Complete

## Milestone 2 Status: Incomplete
Compiles/builds but doesn't run test_heap_storage() correctly

### Errors
When running test_heap_storage(), traced segmentation fault to SlottedPage::address() using cout

Possible errors in HeapTable::validate because not all Python code was included

## How to build
On cs1, clone the repo and build executable by using Makefile
```
$ cd ~/cpsc5300
$ git clone https://github.com/klundeen/5300-Giraffe.git
$ cd 5300-Giraffe
$ make 
```
Run "make clean" to remove files, then run "make" for a clean build

**Remember to have [Berkeley DB data](https://seattleu.instructure.com/courses/1597073/pages/getting-set-up-on-cs1?module_item_id=17258588) in your directory on cs1**
Execute the program using this format
```
$ ./5300-Giraffe ../data
```
You can pull the tag for Milestone 1
```
$ git checkout tags/Milestone1
```
You can pull the tag for Milestone 2
```
$ git checkout tags/Milestone2
```
[Berkeley DB C++ API Reference](https://docs.oracle.com/cd/E17076_05/html/api_reference/CXX/frame_main.html) for db_cxx.h

In heap_storage.cpp, HeapTable did not implement the following functions because they were not required for milestone 2.
* virtual void update(const Handle handle, const ValueDict *new_values);
* virtual void del(const Handle handle);
