# Project: 5300-Giraffe
Student DB Relation Manager project for CPSC5300 at Seattle U, Winter 2024

## Usage

To get started with the 5300-Giraffe project, follow these steps:

1. **Navigate to the Project Directory**: Open a terminal and change to the project directory:

   ```
   cd path/to/5300-Giraffe
   ```

2. **Compile the Program**: Compile the program using the `make` command:

   ```
   make
   ```

3. **Run the Program**: After compiling, you can run the program by specifying the data directory:

   ```
   ./sql5300 [ENV_DIR]
   ```
   Example:
   ```
   ./sql5300 ~/cpsc5300/data/
   ```

   To use Valgrind for detecting memory leaks and errors, run:

   ```
   valgrind ./sql5300 ~/cpsc5300/data/
   ```

Upon running, you'll enter a SQL shell where you can interact with the database.

## Testing Heap Storage

For Milestone 2 and testing the heap storage functionality:

Type `test` to run `test_heap_storage()`, which calls test functions for `HeapFile`, `HeapTable`, and `SlottedPage`.

**Sample Output:**
```
SQL> test
test_heap_storage:

Testing SlottedPage....
SlottedPage::add(): retrieved record 1 successfully
SlottedPage::add(): retrieved record 2 successfully
SlottedPage test passed successfully.

Testing HeapFile....
Created heap file
Opened heap file
Allocate new block passed.
New Block retrieved successfully.
HeapFile dropped.

Testing HeapTable....
create ok
drop ok
create_if_not_exists ok
try insert
insert ok
select ok 1
project ok
Testing HeapTable Done
ok
```

Alternatively, you can type a SQL query to "execute" it. Currently, execution primarily involves parsing the query and printing it back after parsing.

## Dependencies

This project depends on the following:

- **Berkeley DB**: For database storage and management. [Berkeley DB](https://docs.oracle.com/cd/E17076_05/html/api_reference/CXX/frame_main.html) 
- **SQLParser**: For parsing SQL queries. [SQLParser](https://github.com/klundeen/sql-parser/tree/cpsc4300) 

## Heap Storage Engine

The Heap Storage Engine utilizes `SlottedPage` for block architecture, with Berkeley DB's RecNo file type managing each block as one numbered record in the Berkeley DB file. This approach leverages Berkeley DB's buffer manager for disk read/write operations.

## Code Structure

The project is organized into two main directories to separate concerns and maintain a clean project structure:

- **`include/`**: This directory contains all the header files (`.h`). Header files define the interfaces for our classes and functions.

- **`src/`**: This directory contains the implementation files (`.cpp`). These files contain the actual code for the functionalities declared in the header files.

## Cleaning Up

To clean up object files and other build artifacts, run:

```
make clean
```