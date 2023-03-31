Group Memebers:

    1.  Name  : Shikhar Saraswat
        Email : ssaraswat@hawk.iit.edu
        CWID  : A20514418

    2.  Name  : Sadanand Kohle
        Email : skolhe1@hawk.iit.edu
        CWID  : A20517969
        

    3.  Name  : Vidhi Sanghavi
        Email : vsanghavi@hawk.iit.edu
        CWID  : A20467207
        


Project Structure:

assign3
    README .md
    Makefile
    buffer_mgr .h
    buffer_mgr_stat .c
    buffer_mgr_stat .h
    dberror .c
    dberror .h
    dt.h
    storage_mgr .h
    record_mgr.c
    record_mgr.h
    test_assign2_1 .c
    test_assign2_2 .c
    test_helper .h
    test_assign3
    test_assign3_1.c

Project Summary
------------------------------------------------------------------


Steps For Running Assignment3
------------------------------------------------------------------

1) Go to Project root (assign3) using Terminal.

2) Type ls to list the files and check that we are in the correct directory.

3) Type "make clean" to delete old compiled .o files.

4) Type "make" to compile all project files.

5) Type ./test_assign3 to execute test cases.

Function Description
------------------------------------------------------------------

The record manager is started and stopped using associated functions. Use the table-related functions to create, open, shut, and remove a table. Using a page replacement policy, we access pages using Buffer Pool and Buffer Manager (Assignment 2). The Storage Manager (Assignment 1) is indirectly used to perform actions on a disk page file.

initRecordManager (...)
-> This function initializes the record manager.We call initStorageManager(...) function of Storage Manager to initialize the storage manager. 

shutdownRecordManager(...)
->This function terminates the record manager and releases any resources that have been assigned to it. It releases all of the resources and memory that the Record Manager was using. We de-allocate memory by setting the recordManager data structure reference to NULL and using the C function free().

createTable(...)
->The table with the name supplied by the parameter "name" is opened by this function.
By invoking initBufferPool, it starts the buffer pool (...). LRU page replacement is our policy. It also establishes the table's properties (name, datatype, and size), in addition to initializing all of the values in the table. The block containing the table is subsequently written into the page file, which is then opened before being closed.

openTable(...)
-> This function creates a table with name as specified in the parameter 'name' in the schema specified in the parameter 'schema'.


closeTable(...)
->According to the value of the input "rel," this function closes the table. It achieves this by invoking the shutdownBufferPool function of the BUffer Manager ( ). The buffer manager writes the modifications made to the table in the page file before closing the buffer pool.

deleteTable(...)
-> This function deletes the table with name specified by the parameter 'name'. It calls the Storage Manager's function destroyPageFile( ). DestroyPageFile(...) function deletes the page from disk and de-allocates ane memory space allocated for that mechanism.

getNumTuples(...)
-> This function returns the number of tuples in the table .


2. RECORD FUNCTIONS
=======================================

These functions are used to retrieve a record with a certain RID, to delete a record with a certain RID, to insert a new record, and to update an existing record with new values.

insertRecord(...)
-> This function replaces the'record' parameter with the Record ID supplied in the insertRecord() function and puts a record into the table.
For the record being inserted, we set the Record ID.

deleteRecord(...)
-> This function deletes a record having Record ID 'id' send through the parameter from the table and it referenced by the parameter 'rel'.

updateRecord(...)
-> This function updates a record referenced by the parameter "record" in the table referenced by the parameter "rel".

getRecord(....)
-> This function retrieves a record from given  Record ID "id" passed in referenced by "rel" which is  passed in the parameter and the result record is stored at "record" location.



3. SCAN FUNCTIONS
=======================================

Using the Scan-related functions, you can retrieve all tuples from a table that meet a specific requirement (represented as an Expr). The RM ScanHandle data structure given as an input to startScan is initialized when a scan is started. The next function is then called, returning the subsequent tuple that satisfies the scan condition. It returns RC SCAN CONDITION NOT FOUND if NULL is used as a scan condition. after the scan is finished, returns RC RM NO MORE TUPLES, otherwise RC OK (unless an error occurs).

startScan(...)
->The RM_ScanHandle data structure, which is supplied as an input to startScan(), is used by this function to begin a scan. The variables associated to scan in our unique data structure are initialized. If the condition is NULL, an error code is returned. 

next(...)
-> This function returns the next tuple which satisfies the condition (test expression).
-> Return respective error code for null and no tupple.

closeScan(...) 
-> This function closes the scan operation.
-> check if the scan is complete or not. 
-> For incomplete, we unpin the page and reset all scan related variables.
-> We then  de-allocate the space occupied by the metadata.


4. SCHEMA FUNCTIONS
=========================================

These procedures can be used to generate a new schema and return the number of records for a given schema in bytes.

getRecordSize(...)
-> eturns the size of a record in the given schema.
-> Add each attribute's size (space in bytes) requirement iteratively to the variable "size."


freeSchema(...)
->Removes the schema mention in 'schema' parameter from the memory.
-> function free(...) to de-allocate the memory space occupied by the schema.

createSchema(...)
-> This function create a new schema with the specified attributes in memory.


5. ATTRIBUTE FUNCTIONS
=========================================


createRecord(...)
-> This function adds a new record to the schema specified by the parameter "schema" and add the new record to the createRecord() function's "record" parameter.


freeRecord(...)
-> This function de-allocates the memory for given record with the help of c function free().

getAttr(...)
-> Retrieves an attribute from the given record in the specified schema.

setAttr(...)
-> This function sets the attribute value in the record in the specified schema. 
    
