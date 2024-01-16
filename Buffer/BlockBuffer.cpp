#include "BlockBuffer.h"
#include <iostream>
#include <cstdlib>
#include <cstring>

using namespace std;

BlockBuffer::BlockBuffer(int blockNum) {
    this->blockNum = blockNum;
}

RecBuffer::RecBuffer(int blockNum) : BlockBuffer::BlockBuffer(blockNum) {}

int BlockBuffer::getHeader(struct HeadInfo *head) {
    
    unsigned char * bufferPtr = nullptr;

    int ret = loadBlockAndGetBufferPtr(&bufferPtr);

    if(ret != SUCCESS){
        return ret;
    }

    struct HeadInfo * bufferHeader = (struct HeadInfo *) bufferPtr;

    head->blockType = bufferHeader->blockType;
    head->pblock = bufferHeader->pblock;
    head->lblock = bufferHeader->lblock;
    head->rblock = bufferHeader->rblock;
    head->numEntries = bufferHeader->numEntries;
    head->numAttrs = bufferHeader->numAttrs;
    head->numSlots = bufferHeader->numSlots;
    
    return SUCCESS;

}

int BlockBuffer::setHeader(struct HeadInfo *head) {
    unsigned char * bufferPtr = nullptr;
    
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);

    if(ret != SUCCESS) {
        return ret;
    }
    struct HeadInfo *bufferHeader = (struct HeadInfo *) bufferPtr;

    bufferHeader->blockType = head->blockType;
    bufferHeader->pblock = head->pblock;
    bufferHeader->lblock = head->lblock;
    bufferHeader->rblock = head->rblock;
    bufferHeader->numEntries = head->numEntries;
    bufferHeader->numAttrs = head->numAttrs;
    bufferHeader->numSlots = head->numSlots;
    
    StaticBuffer::setDirtyBit(this->blockNum);

    return SUCCESS;
}

int BlockBuffer::setBlockType(int blockType) {
    
    unsigned char * bufferPtr = nullptr;

    int ret = loadBlockAndGetBufferPtr(&bufferPtr);

    if(ret != SUCCESS) {
        return ret;
    }

    *((int32_t *)bufferPtr) = blockType;

    StaticBuffer::blockAllocMap[this->blockNum] = blockType;

    int retVal = StaticBuffer::setDirtyBit(this->blockNum);

    if(retVal != SUCCESS) {
        return retVal;
    }

    return SUCCESS;
}


int BlockBuffer::getFreeBlock(int blockType) {

    int freeBlock = -1;
    for( int i = 0 ; i < DISK_BLOCKS ; ++i ) {
        if(
            StaticBuffer::blockAllocMap[i]==UNUSED_BLK
        ) {
            freeBlock = i;
            break;
        }
    }

    if(freeBlock == -1) {
        return E_DISKFULL;
    }

    this->blockNum = freeBlock;

    int bufferNum = StaticBuffer::getFreeBuffer(this->blockNum);

    struct HeadInfo blockHead;
    
    blockHead.pblock = -1;
    blockHead.lblock = -1;
    blockHead.rblock = -1;
    blockHead.numAttrs = 0;
    blockHead.numEntries = 0;
    blockHead.numSlots = 0;

    setHeader(&blockHead);

    if(blockType == 'R') {
        blockType = REC;
    }
    else if(blockType == 'I') {
        blockType = IND_INTERNAL;
    }
    else if(blockType == 'L') {
        blockType = IND_LEAF;
    }

    setBlockType(blockType);

    return this->blockNum;
}

int BlockBuffer::getBlockNum() {

    return this->blockNum;

}

BlockBuffer::BlockBuffer(char blockType) {
    int allocatedBlock = getFreeBlock(blockType);

    this->blockNum = allocatedBlock;
}

int RecBuffer::getRecord(union Attribute *rec, int slotNum) {
    
    unsigned char * bufferPtr = nullptr;

    int ret = loadBlockAndGetBufferPtr(&bufferPtr);

    if(ret != SUCCESS){
        return ret;
    }

    struct HeadInfo head;

    this->getHeader(&head);

    int attrCount = head.numAttrs;
    int slotCount = head.numSlots;


    int recordSize = attrCount * ATTR_SIZE;
    int offset = HEADER_SIZE + slotCount + (recordSize * slotNum);
    unsigned char *slotPointer = bufferPtr + offset;

    memcpy(rec, slotPointer, recordSize);

    return SUCCESS;
}

int BlockBuffer::loadBlockAndGetBufferPtr(unsigned char **buffPtr) {

    int bufferNum = StaticBuffer::getBufferNum(this->blockNum);

    if(bufferNum != E_BLOCKNOTINBUFFER) {

        StaticBuffer::metainfo[bufferNum].timeStamp = 0;

        for(int bufferIndex = 0; bufferIndex< BUFFER_CAPACITY; ++bufferIndex) {
            if(bufferIndex != bufferNum){
                if(StaticBuffer::metainfo[bufferIndex].free == false) {
                   StaticBuffer::metainfo[bufferIndex].timeStamp++;
                }
            }
        }

    } else {
        bufferNum = StaticBuffer::getFreeBuffer(this->blockNum);

        if(bufferNum == E_OUTOFBOUND) {
            return E_OUTOFBOUND;
        }

        Disk::readBlock(StaticBuffer::blocks[bufferNum], this->blockNum);
    }

    *buffPtr = StaticBuffer::blocks[bufferNum];
    return SUCCESS; 
}

RecBuffer::RecBuffer() : BlockBuffer('R') {}

int RecBuffer::getSlotMap(unsigned char *slotMap) {
    
    unsigned char *bufferPtr = nullptr;

    int ret = loadBlockAndGetBufferPtr(&bufferPtr);

    if(ret != SUCCESS){
        return ret;
    }

    struct HeadInfo head;
    this->getHeader(&head);

    int slotCount = head.numSlots;

    unsigned char * slotMapInBuffer = bufferPtr + HEADER_SIZE;

    memcpy(slotMap, slotMapInBuffer, slotCount);

    return SUCCESS;

}

int RecBuffer::setRecord(union Attribute *rec, int slotNum) {

    unsigned char * bufferPtr = nullptr;

    int ret = loadBlockAndGetBufferPtr(&bufferPtr);

    if (ret != SUCCESS) {
        return ret;
    }

    struct HeadInfo blockHeader;

    this->getHeader(&blockHeader);

    int numAttrs = blockHeader.numAttrs;

    int numSlots = blockHeader.numSlots;

    if(slotNum < 0 || slotNum >= numSlots) {
        return E_OUTOFBOUND;
    }

    int recordSize = ATTR_SIZE * numAttrs;

    unsigned char * slotPointer = bufferPtr + HEADER_SIZE + numSlots + (slotNum * recordSize);

    memcpy(slotPointer, rec, recordSize);

    int retVal  = StaticBuffer::setDirtyBit(this->blockNum);
    
    return SUCCESS;
}

int RecBuffer::setSlotMap(unsigned char *slotMap) {
    
    unsigned char * bufferPtr = nullptr;

    int ret = loadBlockAndGetBufferPtr(&bufferPtr);

    if(ret != SUCCESS) {
        return ret;
    }

    RecBuffer recordBlock(this->blockNum);
    struct HeadInfo blockHeader;

    recordBlock.getHeader(&blockHeader);

    int numSlots = blockHeader.numSlots;
    
    memcpy(bufferPtr + HEADER_SIZE, slotMap, numSlots);

    int retVal = StaticBuffer::setDirtyBit(this->blockNum);

    if(retVal != SUCCESS) {
        return retVal;
    }

    return SUCCESS;
}

void BlockBuffer::releaseBlock() {
    
    if(this->blockNum == INVALID_BLOCKNUM) {
        return;
    }

    int buffNum = StaticBuffer::getBufferNum(this->blockNum);

    if(buffNum != E_BLOCKNOTINBUFFER){
        StaticBuffer::metainfo[buffNum].free = true;
    }
    
    StaticBuffer::blockAllocMap[this->blockNum] = UNUSED_BLK;
    this->blockNum = INVALID_BLOCKNUM;
}

int compareAttrs(union Attribute attr1, union Attribute attr2, int attrType) {

    double diff;
    
    if(attrType == STRING) {
        diff = strcmp(attr1.sVal, attr2.sVal);
    } 
    
    else { 
        diff = attr1.nVal - attr2.nVal;
    }

    if(diff > 0){
        return 1;
    }

    else if(diff < 0) {
        return -1;
    }

    else return 0;

}

IndBuffer::IndBuffer(char blockType) : BlockBuffer(blockType) {}

IndBuffer::IndBuffer(int blockNum) : BlockBuffer(blockNum) {}

IndInternal::IndInternal() : IndBuffer('I') {}

IndInternal::IndInternal(int blockNum) : IndBuffer(blockNum) {}

IndLeaf::IndLeaf() : IndBuffer('L') {}

IndLeaf::IndLeaf(int blockNum) : IndBuffer(blockNum) {}

int IndInternal::getEntry(void *ptr, int indexNum) {

    if(indexNum < 0 || indexNum >= MAX_KEYS_INTERNAL) {
        return E_OUTOFBOUND;
    }

    unsigned char * bufferPtr = nullptr;

    int ret = loadBlockAndGetBufferPtr(&bufferPtr);

    if(ret != SUCCESS) {
        return ret;
    }

    struct InternalEntry *internalEntry = (struct InternalEntry*) ptr;
    unsigned char * entryPtr = bufferPtr + HEADER_SIZE + (indexNum * (sizeof(int) + ATTR_SIZE));

    memcpy(&(internalEntry->lChild), entryPtr, sizeof(int32_t));
    memcpy(&(internalEntry->attrVal), entryPtr + 4, sizeof(Attribute));
    memcpy(&(internalEntry->rChild), entryPtr+ 20, sizeof(int32_t));

    return SUCCESS;
}

int IndLeaf::getEntry(void *ptr, int indexNum) {

    if(indexNum < 0 || indexNum >= MAX_KEYS_INTERNAL) {
        return E_OUTOFBOUND;
    }

    unsigned char * bufferPtr = nullptr;

    int ret = loadBlockAndGetBufferPtr(&bufferPtr);

    if(ret != SUCCESS) {
        return ret;
    }

    unsigned char * entryPtr = bufferPtr + HEADER_SIZE + (indexNum * LEAF_ENTRY_SIZE);
    memcpy((struct Index *)ptr, entryPtr, LEAF_ENTRY_SIZE);

    return SUCCESS;
}

int IndInternal::setEntry(void *ptr, int indexNum){

    if(indexNum < 0 || indexNum >= MAX_KEYS_INTERNAL) {
        return E_OUTOFBOUND;
    }

    unsigned char * bufferPtr = nullptr;

    int ret = loadBlockAndGetBufferPtr(&bufferPtr);

    if(ret != SUCCESS) {
        return ret;
    }

    struct InternalEntry * internalEntry = (struct InternalEntry *) ptr;

    unsigned char * entryPtr = bufferPtr + HEADER_SIZE + indexNum * 20;

    memcpy(entryPtr, &(internalEntry->lChild), 4);
    memcpy(entryPtr + 4 , &(internalEntry->attrVal), ATTR_SIZE);
    memcpy(entryPtr + 20, &(internalEntry->rChild), 4);

    int retVal = StaticBuffer::setDirtyBit(this->blockNum);

    if(retVal < 0) {
        return retVal;
    }

    return SUCCESS;
}

int IndLeaf::setEntry(void *ptr, int indexNum){

    if(indexNum < 0 || indexNum >= MAX_KEYS_INTERNAL) {
        return E_OUTOFBOUND;
    }

    unsigned char * bufferPtr = nullptr;

    int ret = loadBlockAndGetBufferPtr(&bufferPtr);

    if(ret != SUCCESS) {
        return ret;
    }

    unsigned char * entryPtr = bufferPtr + HEADER_SIZE + (indexNum * LEAF_ENTRY_SIZE);
    memcpy(entryPtr, (struct Index *)ptr, LEAF_ENTRY_SIZE);

    int retVal = StaticBuffer::setDirtyBit(this->blockNum);

    if(retVal < 0) {
        return retVal;
    }

    return SUCCESS;
}

