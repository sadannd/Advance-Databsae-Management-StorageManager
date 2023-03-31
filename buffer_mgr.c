#include <stdio.h>
#include <stdlib.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <math.h>

#define ZERO 0
#define ONE 1

int maxPFrames = ZERO;
int numPageCounter = ZERO;
int numPWriteCounter = ZERO;
int numPage = ZERO;
int augmentCall = ZERO;
bool flag = TRUE;
int lastAddedPage = ZERO;
int lFUPage = ZERO;

typedef struct Page
{
  SM_PageHandle smFileDataHandler;
  PageNumber intPageNumber;
  int booleanDirtyBitFlag;
  int fixClientCount;
  int leastRecentlyUsedPageLRU;
  int leastFrequentlyUsedPageLRU;
  int hitNum;
} PageFrame;

void augment(PageFrame *frame, PageFrame *pagePtr, int count)
{
  if (flag)
  {
    pagePtr[count].booleanDirtyBitFlag = (*frame).booleanDirtyBitFlag;
    pagePtr[count].leastRecentlyUsedPageLRU = (*frame).leastRecentlyUsedPageLRU;
    flag = FALSE;
    pagePtr[count].fixClientCount = (*frame).fixClientCount;
    pagePtr[count].intPageNumber = (*frame).intPageNumber;
    augmentCall++;
    pagePtr[count].smFileDataHandler = (*frame).smFileDataHandler;
    flag = TRUE;
  }
}

void FIFO(BM_BufferPool *const bufferPool, PageFrame *page)
{

  int fifoHeadPool = numPageCounter % maxPFrames;
  int count = 0;
  int main = 1;
  PageFrame *pageFramePointer = (PageFrame *)bufferPool->mgmtData;

  do
  {
    if (pageFramePointer[fifoHeadPool].fixClientCount != 0)
    {
      fifoHeadPool = fifoHeadPool + 1;
      bool maxBuffer = (fifoHeadPool % maxPFrames != 0);
      if (!maxBuffer)
        fifoHeadPool = 0;
    }
    else
    {

      if (pageFramePointer[fifoHeadPool].booleanDirtyBitFlag == 1)

        writeBlockOpenPageInvoke(bufferPool, pageFramePointer, fifoHeadPool);
      augment(page, pageFramePointer, fifoHeadPool);
      break;
    }
    count++;
  } while (count < maxPFrames);
}

/*
 * CREATED BY :- Sadanand Kohle
 * NAME: LRU
 * PARAMETERS:
 * * bPool -- Buffer pool.
 * * page -- Page frame to be replaced
 * DESCRIPTION :- Page replacement policy implementation.
 */

void LRU(BM_BufferPool *const bPool, PageFrame *page)
{
  int pageFrameLeast = 0;
  PageFrame *pFrame = (PageFrame *)bPool->mgmtData;
  int leastReachedNumber = pFrame[0].leastRecentlyUsedPageLRU;
  for (int i = 1; i < maxPFrames; i++)
  {
    if (pFrame[i].leastRecentlyUsedPageLRU < leastReachedNumber)
    {
      pageFrameLeast = i;
      leastReachedNumber = pFrame[i].leastRecentlyUsedPageLRU;
    }
  }

  if (pFrame[pageFrameLeast].booleanDirtyBitFlag == 1)
    writeBlockOpenPageInvoke(bPool, pFrame, pageFrameLeast);
  augment(page, pFrame, pageFrameLeast);
}

void LFU(BM_BufferPool *const bufferPool, PageFrame *page)
{
  PageFrame *pageFramePointer = (PageFrame *)bufferPool->mgmtData;
  int lfuIndex = (lFUPage) % maxPFrames;
  int lfuRefrence = pageFramePointer[lfuIndex].leastFrequentlyUsedPageLRU;

  int lfuUpdatedIndex = lfuIndex + 1;
  int pointer = lfuUpdatedIndex % maxPFrames;

  for (int lfuMaxFrameCount = 0; lfuMaxFrameCount < maxPFrames; lfuMaxFrameCount++)
  {
    if (pageFramePointer[pointer].leastFrequentlyUsedPageLRU < lfuRefrence)
    {
      lfuIndex = pointer;
      lfuRefrence = pageFramePointer[pointer].leastFrequentlyUsedPageLRU;
    }
    pointer = (++pointer) % maxPFrames;
  }
  if (pageFramePointer[lfuIndex].booleanDirtyBitFlag == 1)
    writeBlockOpenPageInvoke(bufferPool, pageFramePointer, lfuIndex);
  augment(page, pageFramePointer, lfuIndex);
  lFUPage = lfuIndex + 1;
}

int lastAddedPageBufferPool = 0;

/*
 * CREATED BY :- Shikhar Saraswat
 * NAME: CLOCK
 * PARAMETERS:
 * * bPool -- Buffer pool.
 * * page -- Page frame to be replaced
 * DESCRIPTION :- Page replacement policy implementation.
 */

void CLOCK(BM_BufferPool *const bPool, PageFrame *page)
{

  PageFrame *pageFrame = (PageFrame *)bPool->mgmtData;
  do
  {
    if (lastAddedPageBufferPool % maxPFrames == ZERO)
      lastAddedPageBufferPool = ZERO;

    if ((pageFrame[lastAddedPageBufferPool].booleanDirtyBitFlag == ONE) || (pageFrame[lastAddedPageBufferPool].leastRecentlyUsedPageLRU == ZERO))
    {
      if ((pageFrame[lastAddedPageBufferPool].booleanDirtyBitFlag == ONE))
        writeBlockOpenPageInvoke(bPool, pageFrame, lastAddedPageBufferPool);
      augment(page, pageFrame, lastAddedPageBufferPool);
      lastAddedPageBufferPool = lastAddedPageBufferPool + ONE;
      break;
    }
    else
    {
      pageFrame[lastAddedPageBufferPool++].leastRecentlyUsedPageLRU = ZERO;
    }
  } while (TRUE);
}

/*
 * CREATED BY :- Shikhar Saraswat
 * NAME: setLeastRecentlyUsedPageLRU
 * PARAMETERS:
 * * DESCRIPTION :- Replacing page as per policy selected.
 */

int setLRUPage(ReplacementStrategy method, int leastRecentlyAddedPagesBufferPoolHit, int leastFrequentlyUsedPageLRU)
{
  printf("setLeastRecentlyUsedPageLRU(): \n");

  switch (method)
  {
  case RS_LFU:
    return leastFrequentlyUsedPageLRU + 1;
    break;

  case RS_LRU:
    return leastRecentlyAddedPagesBufferPoolHit;
    break;

  case RS_CLOCK:
    return 1;
    break;

  default:
    break;
  }
}

/*
 * CREATED BY :- Shikhar Saraswat
 * NAME: replacePageFrame
 * PARAMETERS:
 * * bP -- Buffer Pool.
 * * pFrame -- Page Frame
 * * DESCRIPTION :- Replacing page as per policy selected.
 */

void replacePageFrame(BM_BufferPool *const bPool, PageFrame *pFrame)
{
  switch ((*bPool).strategy)
  {
  case RS_LFU:
    LFU(bPool, pFrame);
    break;

  case RS_LRU:
    LRU(bPool, pFrame);
    break;

  case RS_FIFO:
    FIFO(bPool, pFrame);
    break;

  case RS_CLOCK:
    CLOCK(bPool, pFrame);
    break;

  default:
    break;
  }
}

/*
 * CREATED BY :- Shikhar Saraswat
 * NAME: setPagePin
 * PARAMETERS:
 * * pageHandler -- Page handler.
 * * pFrame -- Page frame
 * * pageNum -- Page number to be pinned
 * * DESCRIPTION :- Writing back page on disk.
 */

void setPagePin(BM_PageHandle *const pageHandler, PageFrame *pFrame, const PageNumber pageNum, int fNum)
{
  pFrame[fNum].intPageNumber = pageNum;
  (*pageHandler).data = pFrame[fNum].smFileDataHandler;
  pFrame[fNum].leastRecentlyUsedPageLRU = ZERO;
  (*pageHandler).pageNum = pageNum;
  pFrame[fNum].leastFrequentlyUsedPageLRU = ZERO;
}

/*
 * CREATED BY :- Shikhar Saraswat
 * NAME: pinPage
 * PARAMETERS:
 * * bPool -- Buffer Pool.
 * * page -- Page handler
 * * pNum -- Page number to be pinned
 * * DESCRIPTION :- Writing back page on disk.
 */

RC pinPage(BM_BufferPool *const bPool, BM_PageHandle *const page, const PageNumber pNum)
{
  logger("Info", "Pinning Page");
  PageFrame *pFrame;
  pFrame = (PageFrame *)bPool->mgmtData;
  SM_FileHandle smFileHandler;
  bool isBufferFull = true;

  int maxPage = maxPFrames;
  int i = ZERO;

  do
  {
    if (pFrame[i].intPageNumber < ZERO && i == ZERO)
    {
      openPageFile((*bPool).pageFile, &smFileHandler);

      pFrame[i].smFileDataHandler = (SM_PageHandle)malloc(PAGE_SIZE);
      // int totalPages = smFileHandler.totalNumPages;
      ensureCapacity(pNum, &smFileHandler);

      readBlock(pNum, &smFileHandler, pFrame[i].smFileDataHandler);

      setPagePin(page, pFrame, pNum, i);
      numPageCounter = ZERO;
      lastAddedPage = ZERO;
      pFrame[i].fixClientCount += ONE;

      return RC_OK;
    }
    else
    {
      if (!(pFrame[i].intPageNumber >= ZERO))
      {
        openPageFile((*bPool).pageFile, &smFileHandler);
        pFrame[i].smFileDataHandler = (SM_PageHandle)malloc(PAGE_SIZE);
        readBlock(pNum, &smFileHandler, pFrame[i].smFileDataHandler);

        setPagePin(page, pFrame, pNum, i);
        isBufferFull = false;
        numPageCounter = numPageCounter + ONE;
        lastAddedPage = lastAddedPage + ONE;
        pFrame[i].fixClientCount = ONE;

        if ((*bPool).strategy == RS_CLOCK)
        {
          pFrame[i].leastRecentlyUsedPageLRU = ONE;
        }
        if ((*bPool).strategy == RS_LRU)
        {
          pFrame[i].leastRecentlyUsedPageLRU = lastAddedPage;
        }

        break;
      }
      else
      {
        if (pFrame[i].intPageNumber != pNum)
        {
          printf("page %d could not be found in the memory buffer.\n", pNum);
        }
        else
        {
          pFrame[i].fixClientCount += ONE;
          isBufferFull = false;

          (*page).data = pFrame[i].smFileDataHandler;
          (*page).pageNum = pNum;

          lastAddedPage += ONE;
          lastAddedPageBufferPool += ONE;

          int leastRecentPage = setLRUPage((*bPool).strategy, lastAddedPage, pFrame[i].leastFrequentlyUsedPageLRU);

          if ((*bPool).strategy == RS_LFU)
          {
            pFrame[i].leastFrequentlyUsedPageLRU = leastRecentPage;
          }
          if ((*bPool).strategy == RS_LRU || (*bPool).strategy == RS_CLOCK)
          {
            pFrame[i].leastRecentlyUsedPageLRU = leastRecentPage;
          }
          break;
        }
      }
    }
    i += ONE;
  } while (i < maxPage);

  if (isBufferFull)
  {
    PageFrame *pageFrame = (PageFrame *)malloc(sizeof(PageFrame));
    openPageFile((*bPool).pageFile, &smFileHandler);
    logger("Info", "File opened in Buffer Manager");

    (*pageFrame).smFileDataHandler = (SM_PageHandle)malloc(PAGE_SIZE);
    readBlock(pNum, &smFileHandler, (*pageFrame).smFileDataHandler);

    logger("Info", "File Read in Buffer Manager");
    (*page).data = (*pageFrame).smFileDataHandler;
    (*page).pageNum = pNum;

    lastAddedPage = lastAddedPage + ONE;
    numPageCounter = numPageCounter + ONE;

    (*pageFrame).fixClientCount = ONE;
    (*pageFrame).intPageNumber = pNum;
    (*pageFrame).booleanDirtyBitFlag = ZERO;
    (*pageFrame).leastFrequentlyUsedPageLRU = ZERO;

    if ((*bPool).strategy == RS_CLOCK)
    {
      (*pageFrame).leastRecentlyUsedPageLRU = ONE;
    }
    else if ((*bPool).strategy == RS_LRU)
    {
      (*pageFrame).leastRecentlyUsedPageLRU = lastAddedPage;
    }
    replacePageFrame(bPool, pageFrame);
  }
  return RC_OK;
}

void writeBlockOpenPageInvoke(BM_BufferPool *const bufferPool, PageFrame *pagePointer, int poolPointer)
{
  SM_FileHandle fileHandler;
  openPageFile((*bufferPool).pageFile, &fileHandler);
  logger("Info", "File opened in Buffer Manager");
  int x = pagePointer[poolPointer].intPageNumber;
  writeBlock(x, &fileHandler, pagePointer[poolPointer].smFileDataHandler);
  pagePointer[poolPointer].booleanDirtyBitFlag = ZERO;
  numPage++;
}

PageNumber *getFrameContents(BM_BufferPool *const bufferPool)
{
  PageFrame *framePointer = (PageFrame *)(*bufferPool).mgmtData;
  PageNumber *contentArray = calloc(sizeof(PageNumber), maxPFrames);
  int *countArray = calloc(sizeof(int), maxPFrames);
  for (int contentCount = 0; contentCount < maxPFrames; contentCount++)
  {
    contentArray[contentCount] = framePointer[contentCount].intPageNumber;
  }
  return contentArray;
}

int *getFixCounts(BM_BufferPool *const bufferPool)
{
  PageFrame *framePointer = (PageFrame *)(*bufferPool).mgmtData;
  PageNumber *contentArray = calloc(sizeof(PageNumber), maxPFrames);
  int *countArray = calloc(sizeof(int), maxPFrames);
  for (int maxCount = 0; maxCount < maxPFrames; maxCount++)
  {
    countArray[maxCount] = framePointer[maxCount].fixClientCount;
  }
  return countArray;
}

bool *getDirtyFlags(BM_BufferPool *const bufferPool)
{
  bool *dirtyFlags = calloc(sizeof(bool), maxPFrames);
  PageFrame *pageFramePtr = pageFramePtr = (PageFrame *)(*bufferPool).mgmtData;
  for (int dirtyFlagStart = 0; dirtyFlagStart < maxPFrames; dirtyFlagStart++)
  {
    bool isDirty = (pageFramePtr[dirtyFlagStart].booleanDirtyBitFlag == 1);
    if (!isDirty)
    {
      dirtyFlags[dirtyFlagStart] = false;
    }
    if (isDirty)
    {
      dirtyFlags[dirtyFlagStart] = true;
    }
  }
  return dirtyFlags;
}

int getNumReadIO(BM_BufferPool *const bufferPool)
{
  return (getDirtyFlags);
}

int getNumWriteIO(BM_BufferPool *const bufferPool)
{
  return numPage;
}
/*
 * CREATED BY :- Sadanand Kolhe
 * NAME: initBufferPool
 * PARAMETERS:
 * * bPool -- Buffer Pool.
 * * pgFileName --Filename
 * * totalPages -- Total number of pages
 * * strategy -- Strategy
 * * stratData --Strategy information
 */

RC initBufferPool(BM_BufferPool *const bPool, const char *const pgFileName, const int totalPages, ReplacementStrategy strategy, void *stratData)
{

  if (bPool == NULL || !bPool || !pgFileName)
  {
    return RC_ERROR;
  }
  restoreConstants(totalPages);

  bPool->pageFile = (char *)pgFileName;
  bPool->numPages = totalPages;
  bPool->strategy = strategy;

  PageFrame *pframe = calloc(sizeof(PageFrame), totalPages);

  for (int i = 0; i < totalPages; i++)
  {
    pframe[i].smFileDataHandler = NULL;
    pframe[i].leastRecentlyUsedPageLRU = ZERO;
    pframe[i].intPageNumber = -1;
    pframe[i].leastFrequentlyUsedPageLRU = ZERO;
    pframe[i].booleanDirtyBitFlag = ZERO;
    pframe[i].fixClientCount = ZERO;
  }

  (*bPool).mgmtData = pframe;
  return RC_OK;
}
/*
 * CREATED BY :- Sadanand Kolhe
 * NAME: restoreConstants-Resets the counters
 * PARAMETERS:
 * * totalPages -- Total Pages.
 */

void restoreConstants(int totalPages)
{
  lFUPage = ZERO;
  numPage = ZERO;
  numPageCounter = ZERO;
  maxPFrames = totalPages;
  lastAddedPageBufferPool = ZERO;
}
/*
 * CREATED BY :- Sadanand Kolhe
 * NAME: shutdownBufferPool- Used to close all the pageFrames and release the memory allocated
 * PARAMETERS:
 * * bPool -- Buffer Pool.
 */

RC shutdownBufferPool(BM_BufferPool *const bPool)
{

  if (bPool == NULL || !bPool)
  {
    return RC_ERROR;
  }
  forceFlushPool(bPool);
  PageFrame *pframe;
  pframe = (PageFrame *)bPool->mgmtData;

  int maxs = maxPFrames;
  for (int i = 0; i < maxs; i++)
  {
    if (pframe[i].fixClientCount != 0)
    {
      return RC_ERROR;
    }
  }

  for (int j = 0; j < maxs; j++)
  {
    free(pframe[j].smFileDataHandler);
  }
  free(pframe);
  bPool->mgmtData = NULL;
  logger("Info", "Buffer shut down successfully");

  return RC_OK;
}

/*
 * CREATED BY :- Sadanand Kolhe
 * NAME: forceFlushPool
 * PARAMETERS:
 * * bPool -- Buffer Pool.
 */

RC forceFlushPool(BM_BufferPool *const bPool)
{
  PageFrame *pageFramePtr = (PageFrame *)bPool->mgmtData;

  for (int j = 0; j < maxPFrames; j++)
  {
    if (pageFramePtr[j].fixClientCount == 0 && pageFramePtr[j].booleanDirtyBitFlag == 1)
    {
      writeBlockOpenPageInvoke(bPool, pageFramePtr, j);
    }
  }
  logger("Info", "Force Flush Pool successful");

  return RC_OK;
}

/*
 * CREATED BY :- Sadanand Kolhe
 * NAME: markDirty --Mark the status as a dirty page
 * PARAMETERS:
 * * bPool -- Buffer Pool.
 * * pageHandler -- pageHandler
 */
RC markDirty(BM_BufferPool *const bPool, BM_PageHandle *const pageHandler)
{

  PageFrame *pageFramePointer;
  pageFramePointer = (PageFrame *)bPool->mgmtData;

  for (int i = 0; i < maxPFrames; i++)
  {
    if (pageFramePointer[i].intPageNumber == (*pageHandler).pageNum)
    {
      pageFramePointer[i].booleanDirtyBitFlag = ONE; // Setting dirty flag to 1

      printf("\nPage number: %d, Set dirty flag to 1", pageFramePointer[i].intPageNumber);
      return RC_OK;
    }
  }
  logger("Info", "Current page number do not matched  with the any page in buffer pool");

  return RC_ERROR;
}

/*
 * CREATED BY :- Sadanand Kolhe
 * NAME: unpinPage -- Unpin the page
 * PARAMETERS:
 * * bPool -- Buffer Pool.
 * * pageHandler -- pageHandler
 */
RC unpinPage(BM_BufferPool *const bPool, BM_PageHandle *const pageHandler)
{
  PageFrame *pageFramePointer;
  pageFramePointer = (PageFrame *)bPool->mgmtData;
  for (int i = 0; i < maxPFrames; i++)
  {
    if (pageFramePointer[i].intPageNumber == (*pageHandler).pageNum)
    {
      pageFramePointer[i].fixClientCount--;

      printf("Page number: %d, decremented count by 1 done\n", pageFramePointer[i].intPageNumber);
      return RC_OK;
    }
  }
  logger("Info", "No page Available in Buffer. thus not reducing the count");

  return RC_ERROR;
}

/*
 * CREATED BY :- Sadanand Kolhe
 * NAME: forcePage
 * PARAMETERS:
 * * bPool -- Buffer Pool.
 * * pageHandler -- pageHandler
 */

RC forcePage(BM_BufferPool *const bPool, BM_PageHandle *const pageHandler)
{
  SM_FileHandle smFileHandlePointer;
  PageFrame *pFrame;
  bool flag;
  pFrame = (PageFrame *)bPool->mgmtData;

  for (int i = 0; i < maxPFrames; i++)
  {
    flag = pFrame[i].intPageNumber == (*pageHandler).pageNum;
    if (flag)
    {
      openPageFile((*bPool).pageFile, &smFileHandlePointer);
      writeBlock(pFrame[i].intPageNumber, &smFileHandlePointer, pFrame[i].smFileDataHandler);
      logger("Info", "Block Written in Buffer Manager");
      pFrame[i].booleanDirtyBitFlag = ZERO;
      numPWriteCounter++;
      return RC_OK;
    }
  }
  logger("Info", "No page Available in Buffer");

  return RC_ERROR;
}
