#include <string.h>
#include "storage_mgr.h"
#include "record_mgr.h"
#include <stdio.h>
#include <stdlib.h>
#include "buffer_mgr.h"
#include "logger.c"

#define ONE 1
#define ZERO 0
#define Hundred 100

typedef struct record_mgr
{   
    BM_PageHandle pg_handle;                          
    int tuples;
    RID recordID;                         
    int newPage;
    BM_BufferPool bufferm;                
    int scanTuples;          
    Expr *cond;              
    
} record_mgr;

int numPages = Hundred;
record_mgr *recm;
int attrNames = 15;

int fileUti(int r_capacity, char *data){
    logger("INFO", "in Futilized");
		int index; char fBytes;
		do{
				fBytes = data[index * r_capacity];
                    logger("INFO", "comparing fbytes");
				if (fBytes != '#') // Here we check if the file is empty
					return index; // If not we return the index
				index++;
		}while(index < (PAGE_SIZE/r_capacity)); // We do this until all records have been checked
			logger("INFO", "end of while");
            return -1;
}
extern RC initRecordManager(void *mgmtData)
{

    initStorageManager();
    return RC_OK;
}

extern RC shutdownRecordManager()
{
    free(recm);
    return RC_OK;
}

void pageOperation(char *name, SM_FileHandle fileHandle, char *data)
{
    createPageFile(name);
    openPageFile(name, &fileHandle);
    writeBlock(0, &fileHandle, data);
    closePageFile(&fileHandle);
}

extern RC createTable(char *name, Schema *schema)
{
    recm = (record_mgr *)malloc(sizeof(record_mgr));
    initBufferPool(&recm->bufferm, name, numPages, RS_LRU, NULL);
    SM_FileHandle fileHandle;
    char pageData[PAGE_SIZE];
    char *bufferMgrHandle = pageData;
    int count = 0;
    do
    {
        *(int *)bufferMgrHandle = count;
        bufferMgrHandle = bufferMgrHandle + 4;
        count++;
    } while (count < 2);

    *(int *)bufferMgrHandle = (*schema).numAttr;
    bufferMgrHandle += 4;
    *(int *)bufferMgrHandle = (*schema).keySize;
    bufferMgrHandle += 4;
    int schemaNumberAttr = (*schema).numAttr;

    for (int i = 0; i < (*schema).numAttr; i++)
    {
        strncpy(bufferMgrHandle, schema->attrNames[i], attrNames);
        bufferMgrHandle = bufferMgrHandle + attrNames;
        *(int *)bufferMgrHandle = (int)(*schema).dataTypes[i];
        bufferMgrHandle = bufferMgrHandle + 4;
        *(int *)bufferMgrHandle = (int)(*schema).typeLength[i];
        bufferMgrHandle = bufferMgrHandle + 4;
    }
    bufferMgrHandle = bufferMgrHandle - 4;

    pageOperation(name, fileHandle, pageData);
    return RC_OK;
}

SM_PageHandle setSchema(RM_TableData *rel)
{
    SM_PageHandle pageHandle;
    int attr_count = 0;

    // setting schema properties and incrementing the pointer
    pageHandle = (char *)recm->pg_handle.data;

    (*recm).tuples = *(int *)pageHandle;
    pageHandle = pageHandle + 4;
    (*recm).newPage = *(int *)pageHandle;
    pageHandle = pageHandle + 4;
    attr_count = *(int *)pageHandle;
    pageHandle = pageHandle + 4;

    Schema *schema;
    schema = (Schema *)malloc(sizeof(Schema));
    // setting schema parameters .
    (*schema).numAttr = attr_count;
    (*schema).attrNames = (char **)malloc(sizeof(char *) * attr_count);
    (*schema).dataTypes = (DataType *)malloc(sizeof(DataType) * attr_count);
    (*schema).typeLength = (int *)malloc(sizeof(int) * attr_count);

    int count = 0;
    do
    {
        (*schema).attrNames[count] = (char *)malloc(attrNames);
        count++;
    } while (count < attr_count);

    for (int schemaInd = 0; schemaInd < (*schema).numAttr; schemaInd++)
    {
        strncpy((*schema).attrNames[schemaInd], pageHandle, attrNames);
        pageHandle = pageHandle + attrNames;
        (*schema).dataTypes[schemaInd] = *(int *)pageHandle;
        pageHandle = pageHandle + 4;

        (*schema).typeLength[schemaInd] = *(int *)pageHandle;
        pageHandle = pageHandle + 4;
    }

    (*rel).schema = schema;
    return pageHandle;
}

extern RC openTable(RM_TableData *rmtable, char *name)
{

    if (!name || !rmtable)
    {
        return RC_ERROR;
    }
    (*rmtable).mgmtData = recm;
    (*rmtable).name = name;

    BM_BufferPool *bufPool = &(*recm).bufferm;
    BM_PageHandle *pHandle = &recm->pg_handle;

    if (pinPage(bufPool, pHandle, 0) != RC_OK)
    {
        RC_message = "Pin page failed ";
        return RC_ERROR;
    }
    // schema creation
    SM_PageHandle bufPageHandle = setSchema(rmtable);

    // unpinning page.
    if (unpinPage(bufPool, pHandle) != RC_OK)
    {
        RC_message = "Unpin page failed ";
        return RC_ERROR;
    }
    // forcepage to writeback into the disk.
    forcePage(bufPool, pHandle);
    return RC_OK;
}

extern RC closeTable(RM_TableData *rtable)
{

    if (!rtable)
    {
        return RC_ERROR;
    }
    shutdownBufferPool(&recm->bufferm);
    (*rtable).mgmtData = NULL;
    free(rtable->schema); // free the schema.

    return RC_OK;
}
RC deleteTable(char *name)
{
    if (name == ((char *)0))
    {
        return RC_ERROR;
    }

    RC rc = destroyPageFile(name);
    if (rc == RC_OK)
    {
        return RC_OK;
    }
    else
    {
        return rc;
    }
}


/*
 * CREATED BY :- Sadanand Satish
 * NAME: insertRecord
 */
RC insertRecord(RM_TableData *tbData, Record *rec){ 
	 	char *info, *val;
		record_mgr *recm = tbData->mgmtData;
        logger("INFO", "Setting pointers in insertRecord");
		RID *recpointer = &rec	->	id;
		BM_PageHandle *const pageinfo = &recm -> 	pg_handle;
		BM_BufferPool *const bufferManager = &recm	->	bufferm; 
        logger("INFO", "Pinning page in insert record");
		recpointer	->	page = recm	->	newPage;
		pinPage(bufferManager,pageinfo,recpointer->page); 
		info = recm	->	pg_handle.data;
        logger("INFO", "Pinning page in insert record");
		recpointer	->	slot = fileUti(getRecordSize(tbData	->	schema), info);
		logger("INFO", "File Utilization checked");
 
		while( recpointer ->  slot 	 == -1){
			unpinPage(bufferManager,pageinfo); 
			recpointer	->	page=recpointer->page+1;
            logger("INFO", "Pinning page in insert record");
			pinPage(bufferManager,pageinfo,recpointer->page); 
			info = recm->pg_handle.data; 
			recpointer	->	slot = fileUti(getRecordSize(tbData	->	schema), info);
		    logger("INFO", "End of while in insertRecord");
	 
		}
        logger("INFO", "marking dirty in insert record");

		val = info; 
			markDirty(bufferManager,pageinfo); 
			val = val + (recpointer	->	slot * getRecordSize(tbData	->	schema));	 
			*val = '#'; 
			val=val+1;
            logger("INFO", "memory move in insert record");
			memmove(val, rec->	data + 1, getRecordSize(tbData	->	schema) - 1); 
			unpinPage(bufferManager,pageinfo); 
			recm->	tuples=recm->tuples+1;
			logger("INFO", "Pinning page in insert record");

		pinPage(bufferManager,pageinfo, 0); 
			return RC_OK;
}

extern int getNumTuples(RM_TableData *rel)
{
    return ((record_mgr *)rel->mgmtData)->tuples;
}

/*
 * CREATED BY :- Sadanand Satish
 * NAME: deleteRecord
 */
RC deleteRecord(RM_TableData *tbData, RID rID){ 
		record_mgr *recMgr = tbData->mgmtData; 
		BM_PageHandle *const pageinfo = &recMgr -> pg_handle;
        logger("INFO", "Pinning page in delete record");
		BM_BufferPool *const bufferManager = &recMgr->bufferm; 
		pinPage(bufferManager,pageinfo, rID.page);
        logger("INFO", "setting info in delete record");
        recMgr->newPage	=rID.page; 
		char *info 	= recMgr->pg_handle.data; 
		info = info + (rID.slot * getRecordSize(tbData->schema));
		logger("INFO", "mark dirty delete record");
		markDirty(bufferManager,pageinfo); 
		unpinPage(bufferManager,pageinfo); 
		*info = '*';
        logger("INFO", "setting info to * in delete record");
 
		return RC_OK;
}


/*
 * CREATED BY :- Sadanand Satish
 * NAME: updateRecord
 */
RC updateRecord(RM_TableData *tbData, Record *record){ 
		RID rId; 
		record_mgr *recmgr = tbData->mgmtData;
        logger("INFO", "initialization in update record");
 
		BM_PageHandle *const pageinfo = &recmgr->pg_handle; 
		BM_BufferPool *const bufferManager = &recmgr->bufferm;
		logger("INFO", "Pinning page in update record");
		pinPage(bufferManager,pageinfo,record->id.page);  
		rId=record->id; 
		char *info=recmgr->pg_handle.data;
        logger("INFO", "appending info in update record");
 
		info+=(rId.slot * getRecordSize(tbData->schema));
		*info='#';
		logger("INFO", "mark dirty in update record");
		markDirty(bufferManager,pageinfo);
		info=info+1;
		memmove(info, record->data + 1, getRecordSize(tbData->schema) - 1);
		logger("INFO", "unpin in update record");
		unpinPage(bufferManager,pageinfo); 
		return RC_OK;
}

/*
 * CREATED BY :- Sadanand Satish
 * NAME: getRecord
 */
RC getRecord(RM_TableData *tbData, RID rID, Record *recordInfo){ 
	 	record_mgr *recordMgr = tbData->mgmtData;
        logger("INFO", "fetching record");
 
		BM_PageHandle *const pageinfo = &recm -> pg_handle; 
		BM_BufferPool *const bufferManager = &recm->bufferm;
		logger("INFO", "pin page in get record");
		pinPage(bufferManager,pageinfo, rID.page);  
		char *info = recordMgr->pg_handle.data; 
		info += (rID.slot * getRecordSize(tbData->schema));
        logger("INFO", "updating infor get record");
		if (*info == '#'){ 
		recordInfo->id=rID;  
			char *data1=recordInfo->data;
            logger("INFO", "inside if in getrecords");
			memmove(++data1, info+1, (getRecordSize(tbData->schema) -1)); 
		}else
        {
            return RC_ERROR; 
        }
		logger("INFO", "unpin page in get record");
		unpinPage(bufferManager,pageinfo); 
			return RC_OK;
}


/*
 * CREATED BY :- Sadanand Satish
 * NAME: startScan
 */
RC startScan(RM_TableData *tbData, RM_ScanHandle *scanHandler, Expr *expr){
    logger("INFO", "Starting Scan"); 
	record_mgr *search_cond, *search_record; 
	if (expr !=	NULL){ 
		logger("INFO", "Scanning tables");
		openTable(tbData, "Scanning table"); 
			search_cond = (record_mgr *)malloc(sizeof(record_mgr)); 
			scanHandler->mgmtData = search_cond; 
			(*search_cond).scanTuples = ZERO;	
            (*search_cond).recordID.page = ONE;
			logger("INFO", "initializing variables"); 
			(*search_cond).cond=expr;	
            (*search_cond).recordID.slot=ZERO; 
			(*search_cond).cond = expr; 
			search_record=tbData->mgmtData;
			logger("INFO", "Inilization done for start scanning"); 
            search_record->tuples =attrNames; 
			scanHandler->rel=tbData; 
		return RC_OK;	
	}else 
    {
		return RC_ERROR; 
    }
}

/*
 * CREATED BY :- Sadanand Satish
 * NAME: next
 */

RC next(RM_ScanHandle *scanHandler, Record *record){ 
		logger("INFO", "Finding next scan"); 
		 record_mgr *search_condn, *search_record;
		Schema *tblInfo = scanHandler	->	rel	->	schema; 
		search_record 	= 	scanHandler	->	rel	->	mgmtData;
		logger("INFO", "Initializing variables in next method"); 
		search_condn	= 	scanHandler	->	mgmtData; 
		Value *Output = (Value *)malloc(sizeof(Value));
		logger("INFO", "Iterate loop for multiple occurances"); 
		if ((*search_record).tuples	 == ZERO) 
		{
			logger("INFO", "No more touples");
			return RC_RM_NO_MORE_TUPLES;
		}
		if ((*search_condn).cond 	== 	NULL) 
		{
				logger("INFO", "search_condn is null, returning error");
				return RC_ERROR; 
		}
		do{ 
			logger("INFO", "iterate loop for multiple occurances");
			if ((*search_condn).scanTuples 	<=	 ZERO){
				(*search_condn).recordID.slot 	= 	ZERO;
				(*search_condn).recordID.page 	= 	ONE;
				logger("INFO", "no record update and search again"); 
			}
			else{ 
				(*search_condn).recordID.slot++;
					if ((*search_condn).recordID.slot >= (PAGE_SIZE / getRecordSize(tblInfo))){
						logger("INFO", "no record update and search again"); 
						(*search_condn).recordID.slot = ZERO;
						(*search_condn).recordID.page++; 
					}
			}
			logger("INFO", "init() in next");  
		BM_BufferPool *const buffManager = &search_record->bufferm; 
		BM_PageHandle *const pginfo = &search_condn->pg_handle;
			 
			pinPage(buffManager,pginfo,(*search_condn).recordID.page);
			logger("INFO", "pinning the page in next");  
			char *data = (*search_condn).pg_handle.data; 
			record->id.page = (*search_condn).recordID.page;
			data = data + (*search_condn).recordID.slot * getRecordSize(tblInfo); 
			logger("INFO", "setting values in next");  
			record->id.slot = (*search_condn).recordID.slot; 

			char *dp = record->data;	*dp = '*'; 
			logger("INFO", "mem move start record");  

			memmove(++dp, data + ONE, getRecordSize(tblInfo) - ONE);
			logger("INFO", "mem move in record");  
			search_condn->scanTuples++; 
			evalExpr(record, tblInfo, (*search_condn).cond, &Output); 
				if (Output->v.boolV == TRUE){ 
					logger("INFO", "unpinning the page in next"); 
					unpinPage(buffManager,pginfo); 
						return RC_OK;
				}
			}while(search_condn->scanTuples 	<=	 search_record->tuples);
			logger("INFO", "End of while in next"); 
		BM_BufferPool *const buffManager = &search_record->bufferm; 
		BM_PageHandle *const pginfo = &search_condn->pg_handle;
		unpinPage(buffManager,pginfo); 
		logger("INFO", "Unpinning the page"); 
		(*search_condn).recordID.slot 	=	 ZERO;
		(*search_condn).recordID.page	 =	 ONE;
		(*search_condn).scanTuples 	=	 ZERO;
		logger("INFO", "Returning no more touples"); 
			return RC_RM_NO_MORE_TUPLES;
}

/*
Function: closeScan
Here we free all the memory pointers and data associated with the scan function
Returns OK message in the end
*/
/*
 * CREATED BY :- Sadanand Satish
 * NAME: closeScan
 */
RC closeScan(RM_ScanHandle *scanHandler){ 
		free(scanHandler->mgmtData); 
			return RC_OK;
}

int getRecordSize(Schema *schema)
{
	int size = ZERO;
	if (schema == ((Schema *)0))
		return RC_ERROR;

	for (int index = ZERO; index < (*schema).numAttr; index++)
	{

		if ((*schema).dataTypes[index] == DT_FLOAT)
		{
			logger("INFO", "DataType is Float");
			size += sizeof(float);
		}
		else if ((*schema).dataTypes[index] == DT_INT)
		{
			logger("INFO", "DataType is Int");
			size += sizeof(int);
		}
		else if ((*schema).dataTypes[index] == DT_STRING)
		{
			logger("INFO", "DataType is String");
			size += schema->typeLength[index];
		}
		else if ((*schema).dataTypes[index] == DT_BOOL)
		{
			logger("INFO", "DataType is Bool");
			size += sizeof(bool);
		}
	}
	size = size + 1;
	return size;
}

/*
 * CREATED BY :- Shikhar Saraswat
 * NAME: freeSchema
 * This function frees the memory allocated to schema.
 */

RC freeSchema(Schema *schema)
{
	logger("INFO", "Freeing Schema");
	free(schema);
	return RC_OK;
}

/*
 * CREATED BY :- Shikhar Saraswat
 * NAME: createSchema
 * This function creates a Schema if none exists.
 */

Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
	logger("INFO", "Creating Schema");
	Schema *schema = (Schema *)malloc(sizeof(Schema));
	if (schema == ((Schema *)0))
	{
		return NULL;
	}
	else
	{
		(*schema).keyAttrs = keys;
		(*schema).keySize = keySize;
		(*schema).numAttr = numAttr;
		logger("INFO", "DataType is Bool");
		(*schema).dataTypes = dataTypes;
		(*schema).typeLength = typeLength;
		(*schema).attrNames = attrNames;
		return schema;
	}
}

/*
 * CREATED BY :- Shikhar Saraswat
 * NAME: createRecord
 * This function creates a record.
 */

RC createRecord(Record **record, Schema *schema)
{
	Record *recordObject = (Record *)malloc(sizeof(Record));
	(*recordObject).data = (char *)malloc(getRecordSize(schema));
	logger("INFO", "Allocating memory to record!");
	(*recordObject).id.slot = -ONE;
	(*recordObject).id.page = -ONE;
	char *data = (*recordObject).data;
	*data = '*';
	logger("INFO", "Setting data pointer");
	data++;
	*(data) = '\0';
	*record = recordObject;
	return RC_OK;
}

/*
 * CREATED BY :- Shikhar Saraswat
 * NAME: createSchema
 * This function sets the pointer as per the offset according to the size.
 */

RC DTOffset(Schema *schema, int attrNum, int *offset)
{
	*offset = ONE;
	for (int count = ZERO; count < attrNum; count++)
	{
		if ((*schema).dataTypes[count] == DT_STRING)
		{
			logger("INFO", "DataType is String in DTOoffset");
			*offset += schema->typeLength[count];
		}
		else if ((*schema).dataTypes[count] == DT_BOOL)
		{
			logger("INFO", "DataType is Bool in DTOoffset");
			*offset += sizeof(bool);
		}
		else if ((*schema).dataTypes[count] == DT_FLOAT)
		{
			logger("INFO", "DataType is FLOAT in DTOoffset");
			*offset += sizeof(float);
		}
		else if ((*schema).dataTypes[count] == DT_INT)
		{
			logger("INFO", "DataType is Integer in DTOoffset");
			*offset += sizeof(int);
		}
	}
	return RC_OK;
}

/*
 * CREATED BY :- Shikhar Saraswat
 * NAME: freeRecord
 * This function deallocates the memory allocated to the record.
 */

RC freeRecord(Record *record)
{
	logger("INFO", "Freeing Record!");
	free(record);
	return RC_OK;
}

/*
 * CREATED BY :- Shikhar Saraswat
 * NAME: getAttr
 * This function fetches attribute of a record from a specified schema.
 */

RC getAttr(Record *record, Schema *schema, int attrNum, Value **value)
{
	int offset = ZERO;
	DTOffset(schema, attrNum, &offset);
	char *data = record->data;
	data = data + offset;
	Value *column = (Value *)malloc(sizeof(Value));
	char *dp = record->data;
	dp += offset;

	if (attrNum != ONE)
		(*schema).dataTypes[attrNum] = (*schema).dataTypes[attrNum];
	else
		(*schema).dataTypes[attrNum] = ONE;

	if ((*schema).dataTypes[attrNum] == DT_INT)
	{
		int value;
		value = ZERO;
		logger("INFO", "DataType Int in getAttr");
		column->dt = DT_INT;
		memmove(&value, dp, sizeof(int));
		logger("INFO", "Memmove in getAttr");
		column->v.intV = value;
	}
	else if ((*schema).dataTypes[attrNum] == DT_BOOL)
	{
		bool value;
		logger("INFO", "DataType Bool in getAttr");
		column->dt = DT_BOOL;
		memmove(&value, dp, sizeof(bool));
		column->v.boolV = value;
	}

	switch (schema->dataTypes[attrNum])
	{
	case DT_FLOAT:
		column->dt = DT_FLOAT;
		float value;
		memmove(&value, dp, sizeof(float));
		column->v.floatV = value;
		break;

	case DT_STRING:
		column->dt = DT_STRING;
		int length = schema->typeLength[attrNum];
		column->v.stringV = (char *)malloc(length + ONE);
		strncpy(column->v.stringV, dp, length);
		column->v.stringV[length] = '\0';
		break;
	}
	*value = column;
	return RC_OK;
} 

/*
 * CREATED BY :- Shikhar Saraswat
 * NAME: setAttr
 * This function sets the attribute in the record.
 */

RC setAttr(Record *record, Schema *schema, int attrNum, Value *value)
{
	int index = ZERO;
	int length;
	DTOffset(schema, attrNum, &index);
	char *data = record->data;
	data += index;

	switch ((*schema).dataTypes[attrNum])
	{

	case DT_BOOL:
		*(bool *)data = value->v.boolV;
		data += sizeof(bool);
		break;

	case DT_STRING:
		length = (*schema).typeLength[attrNum];
		logger("INFO", "Copying String in setAttr");
		strncpy(data, value->v.stringV, length);
		data += (*schema).typeLength[attrNum];
		break;

	case DT_FLOAT:
		*(float *)data = value->v.floatV;
		data += sizeof(float);
		break;

	case DT_INT:
		*(int *)data = value->v.intV;
		data += sizeof(int);
		break;
	}
	return RC_OK;
}