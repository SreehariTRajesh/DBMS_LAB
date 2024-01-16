#include "StaticBuffer.h"
#include <cstdio>
#include "../Disk_Class/Disk.h"
#include <cstring>

unsigned char StaticBuffer::blocks[BUFFER_CAPACITY][BLOCK_SIZE];
struct BufferMetaInfo StaticBuffer::metainfo[BUFFER_CAPACITY];
unsigned char StaticBuffer::blockAllocMap[DISK_BLOCKS];

StaticBuffer::StaticBuffer() {

    for( int i = 0; i < BLOCK_ALLOCATION_MAP_SIZE ; ++i ) {
        
        unsigned char blk[BLOCK_SIZE];
        memset(blk, '\0', sizeof(blk));
        Disk::readBlock(blk, i);
        
        for(int j = 0 ; j < BLOCK_SIZE ; ++j) {
            blockAllocMap[i * BLOCK_SIZE + j] = blk[j];
        }
    }

    for(int bufferIndex = 0; bufferIndex < BUFFER_CAPACITY ; ++bufferIndex) {
        metainfo[bufferIndex].free = true;
        metainfo[bufferIndex].dirty = false;
        metainfo[bufferIndex].timeStamp = -1;
        metainfo[bufferIndex].blockNum = -1;
    }

}

StaticBuffer::~StaticBuffer() {
    
    for( int i = 0; i < BLOCK_ALLOCATION_MAP_SIZE ; ++i ) {
        
        unsigned char blk[BLOCK_SIZE];
        memset(blk, '\0', sizeof(blk));

        for(int j = 0 ; j < BLOCK_SIZE ; ++j) {
            blk[j] = blockAllocMap[i * BLOCK_SIZE + j];
        }
        Disk::writeBlock(blk, i);
    }

    for(int bufferIndex = 0; bufferIndex < BUFFER_CAPACITY ; ++bufferIndex) {
        if(metainfo[bufferIndex].free == false && metainfo[bufferIndex].dirty == true) {
            Disk::writeBlock(blocks[bufferIndex], metainfo[bufferIndex].blockNum);
        }
    }
}

int StaticBuffer::getFreeBuffer(int blockNum) {
    if(blockNum < 0 || blockNum > DISK_BLOCKS ) {
        return E_OUTOFBOUND;
    }
    
    for(int bufferIndex = 0; bufferIndex < BUFFER_CAPACITY ; ++bufferIndex) {
        if(metainfo[bufferIndex].free == false) {
            metainfo[bufferIndex].timeStamp++;
        }
    }

    int allocatedBuffer = -1;
    for(int bufferIndex = 0; bufferIndex < BUFFER_CAPACITY ; ++bufferIndex) {
        if(metainfo[bufferIndex].free == true) {
            allocatedBuffer = bufferIndex;
            break;
        }
    }
    
    if(allocatedBuffer == -1) {
        int maxTimeStamp=0;
        int maxIndex=-1;
        for(int bufferIndex = 0; bufferIndex < BUFFER_CAPACITY ; ++bufferIndex) {
            if(metainfo[bufferIndex].timeStamp >= maxTimeStamp) {
                maxTimeStamp = metainfo[bufferIndex].timeStamp;
                maxIndex = bufferIndex;
            }
        }

        if(metainfo[maxIndex].dirty == true) {
            Disk::writeBlock(blocks[maxIndex], metainfo[maxIndex].blockNum);
        }
        allocatedBuffer = maxIndex;
    }

    metainfo[allocatedBuffer].free = false;
    metainfo[allocatedBuffer].dirty = false;
    metainfo[allocatedBuffer].blockNum = blockNum;
    metainfo[allocatedBuffer].timeStamp = 0;
    
    return allocatedBuffer;
}

int StaticBuffer::getBufferNum(int blockNum) {
    if(blockNum < 0 || blockNum > DISK_BLOCKS) {
        return E_OUTOFBOUND;
    }
    for(int bufferIndex = 0; bufferIndex < BUFFER_CAPACITY ; ++bufferIndex){
        if( metainfo[bufferIndex].blockNum == blockNum ) {
            return bufferIndex;
        }
    }
    return E_BLOCKNOTINBUFFER;
}

int StaticBuffer::setDirtyBit(int blockNum) {

    int bufferNum = StaticBuffer::getBufferNum(blockNum);

    if(bufferNum == E_BLOCKNOTINBUFFER) {
        return E_BLOCKNOTINBUFFER;
    }

    if(bufferNum == E_OUTOFBOUND) {
        return E_OUTOFBOUND;
    }

    else {
        metainfo[bufferNum].dirty = true;
    }
    return SUCCESS;
}

int StaticBuffer::getStaticBlockType(int blockNum) {
    
    if(blockNum <= 0 || blockNum >= DISK_BLOCKS) {
        return E_OUTOFBOUND;
    }

    int blockType = (int) blockAllocMap[blockNum];
    
    return blockType;
}
