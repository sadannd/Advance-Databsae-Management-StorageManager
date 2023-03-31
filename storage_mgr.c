#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <string.h>
#include <math.h>
#include <stdbool.h>
#include "storage_mgr.h"
//#include "logger.c"

#define ONE 1
#define ZERO 0
#define TWO_HUNDRED 200

FILE *my_File_Ptr;

extern void initStorageManager (void){
    my_File_Ptr = NULL;     //initialise this pointer to null to clear previous references if any
}

/*
 * CREATED BY :- Sadanand Kolhe
 * NAME: createPageFile
 * PARAMETERS:
 * smPageFileName -- Reference to Filename.
 */

RC createPageFile(char *smPageFileName)
{
    FILE *filePointer = fopen(smPageFileName, "wb+");

    if (filePointer == NULL)
    {
        logger("Error", "Unable to find the file at given location!");
        return RC_FILE_NOT_FOUND;  //Return File not found error if file pointer is null             
    }

    SM_PageHandle *pagePtr = (SM_PageHandle *)calloc(PAGE_SIZE, ONE); //Initializing page pointer using calloc to store one page of size PAGE_SIZE


    if (fwrite(pagePtr, PAGE_SIZE, 1, filePointer) < 1)  //writing data to file pointed by pagePtr to the filePointer stream.
    {
        return RC_WRITE_FAILED;   //return Error if write operation fails
    }

    fclose(filePointer); //Closing the File
    free(pagePtr);   //deallocating the memory allocated by calloc

    return RC_OK;
}

/*
 * CREATED BY :- Sadanand Kolhe
 * NAME: openPageFile
 * PARAMETERS:
 * *smPageFileName -- Reference to Filename.
 * *storageManagerFileHandler -- Reference to the FileHandler
 */

RC openPageFile(char *smPageFileName, SM_FileHandle *storageManagerFileHandler)
{
    struct stat fileMetaData;

    my_File_Ptr = fopen(smPageFileName, "r");
    if (my_File_Ptr == NULL)
    {
        logger("Error", "Unable to find the file at given location!");
        return RC_FILE_NOT_FOUND;
    }
    (*storageManagerFileHandler).fileName = smPageFileName; // assign filename to filehandler->filename
    (*storageManagerFileHandler).curPagePos = 0;    // first page is page 0

    int fStatus = fstat(fileno(my_File_Ptr), &fileMetaData);  //obtaining information about an open file.
    if (fStatus == -1)
    {
        return RC_READ_NON_EXISTING_PAGE; //Returning Error if the file is not linked properly or does not exists
    }
    //(*storageManagerFileHandler).mgmtInfo=my_File_Ptr; // Storing file pointer in the mgmtInfo

    (*storageManagerFileHandler).totalNumPages = fileMetaData.st_size / PAGE_SIZE;  // assign the totalNumPages value to filehandler->totalNumPages
    fclose(my_File_Ptr);

    return RC_OK;
}

/*
 * CREATED BY :- Sadanand Kolhe
 * NAME: closePageFile
 * PARAMETERS:
 * *storageManagerFileHandler -- Reference to the FileHandler
 */

extern RC closePageFile (SM_FileHandle *smFileHandle)
{
    if (my_File_Ptr != NULL) {
        my_File_Ptr = NULL;
        printf("File closed Succesfully!\n");
        return RC_OK;
    }

    bool isFileClosed = fclose(my_File_Ptr);

    if (isFileClosed == 0) {
        return RC_OK;
    }
    return RC_FILE_NOT_FOUND;
}

/*
 * CREATED BY :- Sadanand Kolhe
 * NAME: destroyPageFile
 * PARAMETERS:
 * *smPageFileName -- Reference to Filename.
 */

RC destroyPageFile(char *smPageFileName)
{
    int statusCode = remove(smPageFileName);

    if (statusCode == 0)
    {
        logger("Success", "File Destroyed successfully");
        return RC_OK;
    }
    logger("Error", "Unexpected error occured while destroying file!");

    return RC_ERROR;
}

/*
 * CREATED BY :- Shikhar Saraswat
 * NAME: readBlock
 * PARAMETERS:
 * * pagePos -- The Numnber/position of the page.
 * * SM_FileHandle -- Reference to the FileHandler
 * * SM_PageHandle -- Reference to the PageHandler
 */

RC readBlock(int pagePos, SM_FileHandle *FileHandler, SM_PageHandle Page)
{

    int position;
    long offset = pagePos * PAGE_SIZE;
    int CHAR = sizeof(char);

    if (FileHandler->totalNumPages < pagePos)
    {
        logger("Error", "The page requested to read does not exist!");
        return RC_READ_NON_EXISTING_PAGE;
    }

    my_File_Ptr = fopen(FileHandler->fileName, "r");

    if (my_File_Ptr == NULL)
    {
        logger("Error", "The requested file cannot be opened");
        return RC_FILE_NOT_FOUND;
    }

    logger("Success", "File successfully opened in read mode");
    position = fseek(my_File_Ptr, offset, SEEK_SET);

    if (position != ZERO)
    {
        logger("Error", "Setting pointer position unsuccessful");
        exit(ONE);
    }

    printf("Pointer successfuly set at %ld\n", ftell(my_File_Ptr));
    fread(Page, CHAR, PAGE_SIZE, my_File_Ptr);

    printf("Post read position of the Pointer : %ld\n", ftell(my_File_Ptr));
    FileHandler->curPagePos = ftell(my_File_Ptr);

    int close = fclose(my_File_Ptr);

    if (close == ZERO)
    {
        logger("Success", "File closed");
        return RC_OK;
    }
    else
    {
        logger("Error", "Unsuccessful closing of the file");
        exit(ONE);
    }
}

/*
 * CREATED BY :- Shikhar Saraswat
 * NAME: readBlock
 * PARAMETERS:
 * * SM_FileHandle -- Reference to the FileHandler
 */

int getBlockPos(SM_FileHandle *FileHandler)
{

    if (FileHandler)
    {
        char str[TWO_HUNDRED];
        sprintf(str, "getBlockPos(): Block position is %d", FileHandler->curPagePos);
        logger("Info", str);
        return FileHandler->curPagePos;
    }
    else
    {
        logger("ERROR", "getBlockPos(): File handler is not initiated.");
        return ZERO;
    }
}

/*
 * CREATED BY :- Shikhar Saraswat
 * NAME: readFirstBlock
 * PARAMETERS:
 * * SM_FileHandle -- Reference to the FileHandler
 * * SM_PageHandle -- Reference to the PageHandler
 */

RC readFirstBlock(SM_FileHandle *FileHandler, SM_PageHandle Page)
{

    return readBlock(ZERO, FileHandler, Page);
}

/*
 * CREATED BY :- Shikhar Saraswat
 * NAME: readPreviousBlock
 * PARAMETERS:
 * * SM_FileHandle -- Reference to the FileHandler
 * * SM_PageHandle -- Reference to the PageHandler
 */

RC readPreviousBlock(SM_FileHandle *FileHandler, SM_PageHandle Page)
{

    int prevPage = (FileHandler->curPagePos / PAGE_SIZE) - ONE;
    return readBlock(prevPage, FileHandler, Page);
}

/*
 * CREATED BY :- Shikhar Saraswat
 * NAME: readCurrentBlock
 * PARAMETERS:
 * * SM_FileHandle -- Reference to the FileHandler
 * * SM_PageHandle -- Reference to the PageHandler
 */

RC readCurrentBlock(SM_FileHandle *FileHandler, SM_PageHandle Page)
{

    return readBlock((FileHandler->curPagePos / PAGE_SIZE), FileHandler, Page);
}

/*
 * CREATED BY :- Shikhar Saraswat
 * NAME: readNextBlock
 * PARAMETERS:
 * * SM_FileHandle -- Reference to the FileHandler
 * * SM_PageHandle -- Reference to the PageHandler
 */

RC readNextBlock(SM_FileHandle *FileHandler, SM_PageHandle Page)
{

    int nextPage = (FileHandler->curPagePos / PAGE_SIZE) + ONE;
    return readBlock(nextPage, FileHandler, Page);
}

/*
 * CREATED BY :- Shikhar Saraswat
 * NAME: readLastBlock
 * PARAMETERS:
 * * SM_FileHandle -- Reference to the FileHandler
 * * SM_PageHandle -- Reference to the PageHandler
 */

RC readLastBlock(SM_FileHandle *FileHandler, SM_PageHandle Page)
{

    return readBlock((FileHandler->totalNumPages - ONE), FileHandler, Page);
}

int exists(const char *fileName)
{
    int exist = (access(fileName, F_OK) != -1);
    int ans = exist == 1 ? 1 : 0;
    return ans;
}

RC writeBlock(int pageNum, SM_FileHandle *fileHandle, SM_PageHandle memoryHandle)
{
    if (!exists(fileHandle->fileName))
        return RC_FILE_NOT_FOUND;
    if (pageNum < 0)
        return RC_READ_NON_EXISTING_PAGE;
    FILE *fp;
    fp = fopen(fileHandle->fileName, "r+");
    int sucess = fseek(fp, (pageNum * PAGE_SIZE), SEEK_SET);
    if (sucess != 0)
        return RC_WRITE_FAILED;
    fwrite(memoryHandle, sizeof(char), PAGE_SIZE, fp);

    fileHandle->curPagePos = ftell(fp);
    fclose(fp);
    return RC_OK;
}

RC writeCurrentBlock(SM_FileHandle *fileHandle, SM_PageHandle memoryHandle)
{
    writeBlock(fileHandle->curPagePos, fileHandle, memoryHandle);
    return RC_OK;
}

RC appendEmptyBlock(SM_FileHandle *fileHandle)
{
    if (!exists(fileHandle->fileName))
        return RC_FILE_NOT_FOUND;

    char block[PAGE_SIZE];
    writeBlock(fileHandle->totalNumPages, fileHandle, block);
    return RC_OK;
}

int TotalNumPages(struct stat *);

RC ensureCapacity(int numberOfPages, SM_FileHandle *fileHandle)
{
    if (!exists(fileHandle->fileName))
        return RC_FILE_NOT_FOUND;
    if (numberOfPages > fileHandle->totalNumPages)
    {
        int requiredPages = numberOfPages - fileHandle->totalNumPages;
        char dummyBlock[PAGE_SIZE];
        int j = 0;

        while (j < PAGE_SIZE)
        {
            dummyBlock[j] = 0;
            j++;
        }
        for (int i = 1; i <= requiredPages; i++)
        {
            fwrite(dummyBlock, PAGE_SIZE, 1, fileHandle->fileName);
        }
    }

    return RC_OK;
}