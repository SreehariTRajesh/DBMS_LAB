#include "BPlusTree.h"
#include <cstring>
#include <cstdio>

RecId BPlusTree::bPlusSearch(int relId, char attrName[ATTR_SIZE], Attribute attrVal, int op) {

    IndexId searchIndex;
    AttrCacheTable::getSearchIndex(relId, attrName, &searchIndex);

    AttrCatEntry attrCatEntry;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

    int attrType = attrCatEntry.attrType;

    int block, index;

    if( searchIndex.block == -1 && searchIndex.index == -1) {
        
        block = attrCatEntry.rootBlock;
        index = 0;
        
        if(block == -1) {
            return RecId{-1, -1};
        }
 
    } else {
        
        block = searchIndex.block;
        index = searchIndex.index + 1;

        IndLeaf leaf(block);

        struct HeadInfo leafHead;

        leaf.getHeader(&leafHead);

        if(index >= leafHead.numEntries) {

            block = leafHead.rblock;
            index = 0;

            if(block == -1) {
                return RecId{-1, -1};
            }

        }
    }
    // Search starts from the root block

    while(StaticBuffer::getStaticBlockType(block) == IND_INTERNAL) {
        
        IndInternal internalBlk(block);
        struct HeadInfo intHead;

        internalBlk.getHeader(&intHead);

        InternalEntry intEntry;

        if( op == NE || op == LT || op == LE ) {

            internalBlk.getEntry((void*) &intEntry, 0);
            block = intEntry.lChild;
        
        } else {

            bool flag = false;
            for(int i = 0 ; i < intHead.numEntries ; ++i) {
            
                internalBlk.getEntry((void*) &intEntry, i);
                int cmpVal = compareAttrs(intEntry.attrVal, attrVal, attrType);
                if(( ( op == GE || op == EQ) && cmpVal >= 0 ) || ( op == GT && cmpVal > 0 ) ) {
                    flag = true;
                    break;
                } 
            
            }
            
            if(flag == true) {     
                // Found an entry which is greater than attrVal;
                // Set Block Number to left Child of the internal Entry;
                block = intEntry.lChild;
         
            } else {
        
                InternalEntry lastEntry;
                internalBlk.getEntry((void *)&lastEntry, intHead.numEntries-1);
                block = lastEntry.rChild;
                // Set Block Number to the right Child of the internal Entry;
            }
        
        }
    }

    while(block != -1) {

        IndLeaf leafBlk(block);
        HeadInfo leafHead;

        leafBlk.getHeader(&leafHead);

        Index leafEntry;

        while(index < leafHead.numEntries) {

            leafBlk.getEntry((void *) &leafEntry, index);
            int cmpVal = compareAttrs(leafEntry.attrVal, attrVal, attrType);

            if (
                (op == EQ && cmpVal == 0) ||
                (op == LE && cmpVal <= 0) ||
                (op == LT && cmpVal < 0) ||
                (op == GT && cmpVal > 0) ||
                (op == GE && cmpVal >= 0) ||
                (op == NE && cmpVal != 0)
            ) {

                searchIndex.block = block;
                searchIndex.index = index;
            
                AttrCacheTable::setSearchIndex(relId, attrName, &searchIndex);
                return RecId{leafEntry.block, leafEntry.slot};
            
            } else if((op == EQ || op == LE || op == LT) && cmpVal > 0) {

                return RecId{-1, -1};
            
            }

            index++;
        }

        if (op != NE) {
            break;
        }
        
        block = leafHead.rblock;
        index = 0;
    }

    return RecId{-1, -1};
}

int BPlusTree::bPlusCreate(int relId, char attrName[ATTR_SIZE]) {

    if(relId == RELCAT_RELID || relId == ATTRCAT_RELID) {
        return E_NOTPERMITTED;
    }

    AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

    if (ret != SUCCESS) {
        return ret;
    }

    if(attrCatEntry.rootBlock != -1) {
        return SUCCESS;
    }

    IndLeaf rootBlockBuf;

    int rootBlock = rootBlockBuf.getBlockNum();
    
    int attrOffset = attrCatEntry.offset;

    attrCatEntry.rootBlock = rootBlock;

    AttrCacheTable::setAttrCatEntry(relId, attrName, &attrCatEntry);

    if(rootBlock == E_DISKFULL) {
        return E_DISKFULL;
    }

    RelCatEntry relCatEntry;

    RelCacheTable::getRelCatEntry(relId, &relCatEntry);

    int block = relCatEntry.firstBlk;

    while(block != -1) {

        RecBuffer recordBlk(block);

        unsigned char slotMap[relCatEntry.numSlotsPerBlk];

        recordBlk.getSlotMap(slotMap);

        for(int i = 0 ; i < relCatEntry.numSlotsPerBlk ; ++i) {
            
            if(slotMap[i] != SLOT_UNOCCUPIED) {
                Attribute record[relCatEntry.numAttrs];
                recordBlk.getRecord(record, i);
                
                RecId recId{block, i};

                int retVal = BPlusTree::bPlusInsert(relId, attrName, record[attrOffset], recId);

                if(retVal == E_DISKFULL) {
                    return E_DISKFULL;
                }
            }
        }

        struct HeadInfo recordBlkHeader;
        recordBlk.getHeader(&recordBlkHeader);
        block = recordBlkHeader.rblock;
    }

    return SUCCESS;
}

int BPlusTree::bPlusDestroy(int rootBlockNum) {

    if( rootBlockNum < 0 || rootBlockNum >= DISK_BLOCKS) {
        return E_OUTOFBOUND;
    }

    int type = StaticBuffer::getStaticBlockType(rootBlockNum);

    if(type == IND_LEAF) {
        
        IndLeaf rootBlk(rootBlockNum);
        rootBlk.releaseBlock();
        return SUCCESS;

    }
    else if(type == IND_INTERNAL) {

        IndInternal rootBlk(rootBlockNum);
        
        struct HeadInfo rootBlkHeader;
        rootBlk.getHeader(&rootBlkHeader);

        InternalEntry firstEntry;
        rootBlk.getEntry((void *) &firstEntry, 0);

        BPlusTree::bPlusDestroy(firstEntry.lChild);
        
        for(int i = 0 ; i < rootBlkHeader.numEntries ; ++i) {
            InternalEntry entry;
            rootBlk.getEntry((void *) &entry, i);
            BPlusTree::bPlusDestroy(entry.rChild);
        }

        rootBlk.releaseBlock();
    
    }

    return E_INVALIDBLOCK;

}

int BPlusTree::bPlusInsert(int relId, char attrName[ATTR_SIZE], Attribute attrVal, RecId recId) {
    
    AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);
    
    if(ret != SUCCESS) {
        return ret;
    }   

    int blockNum = attrCatEntry.rootBlock;
    int type = attrCatEntry.attrType;

    if(blockNum == -1) {
        return E_NOINDEX;
    }
    
    
    int leafBlk = BPlusTree::findLeafToInsert(blockNum, attrVal, type);
    
    struct Index entry;

    entry.attrVal.nVal = attrVal.nVal;
    strcpy(entry.attrVal.sVal, attrVal.sVal);
    
    entry.block = recId.block;
    entry.slot = recId.slot;
    
    int insertStatus = BPlusTree::insertIntoLeaf(relId, attrName, leafBlk, entry);
    
    if(insertStatus == E_DISKFULL) {

        BPlusTree::bPlusDestroy(blockNum);
        attrCatEntry.rootBlock = -1;
        AttrCacheTable::setAttrCatEntry(relId, attrName, &attrCatEntry);
        return E_DISKFULL;

    }

    return SUCCESS;
}

int BPlusTree::findLeafToInsert(int rootBlock, Attribute attrVal, int attrType) {
    
    int blockNum = rootBlock;
    
    while( StaticBuffer::getStaticBlockType(blockNum) != IND_LEAF ) {
    
        IndInternal intBlk(blockNum);
    
        struct HeadInfo intBlkHeader;

        intBlk.getHeader(&intBlkHeader);
        
        bool flag = false;

        struct InternalEntry firstEntryGreaterThanAttrVal;

        for(int i = 0 ; i < intBlkHeader.numEntries ; ++i) {
            struct InternalEntry entry;
            intBlk.getEntry((void *) &entry, i);
            int cmpVal = compareAttrs(entry.attrVal, attrVal, attrType);
            if(cmpVal >= 0) {
                firstEntryGreaterThanAttrVal = entry;
                flag = true;
                break;
            }
        }

        if( flag == false) { 
        
            struct InternalEntry lastEntry;   
            intBlk.getEntry((void *) &lastEntry, intBlkHeader.numEntries - 1);    
            blockNum = lastEntry.rChild;
        
        } 
        
        else {
        
            blockNum = firstEntryGreaterThanAttrVal.lChild;
        
        }
    }

    return blockNum;
}

int BPlusTree::insertIntoLeaf(int relId, char attrName[ATTR_SIZE], int blockNum, Index indexEntry) {
    
    AttrCatEntry attrCatEntry;

    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

    IndLeaf leafIndexBlk(blockNum);

    struct HeadInfo leafBlkHeader;

    leafIndexBlk.getHeader(&leafBlkHeader);

    Index indices[leafBlkHeader.numEntries + 1];
    
    if(leafBlkHeader.numEntries == 0){

        indices[0] = indexEntry;
    
    } else {

        bool firstOccurence = false;
        int j = 0;

        for(int i = 0 ; i < leafBlkHeader.numEntries ; ++i ) {
            
            Index entry;
            leafIndexBlk.getEntry((void *) &entry, i);

            int cmpVal = compareAttrs(entry.attrVal, indexEntry.attrVal, attrCatEntry.attrType);
            
            if (cmpVal >= 0 && firstOccurence == false) {
                firstOccurence = true;
                indices[j]= indexEntry;
                ++j;
            }
        
            indices[j] = entry;     
            ++j;
        }
        if(firstOccurence == false){
            indices[leafBlkHeader.numEntries] = indexEntry;
        }
    }
    
    if (leafBlkHeader.numEntries != MAX_KEYS_LEAF) {
    
        leafBlkHeader.numEntries++;
        leafIndexBlk.setHeader(&leafBlkHeader);

        for(int i = 0 ; i <  leafBlkHeader.numEntries ; ++i) {
            leafIndexBlk.setEntry((void *) &indices[i], i);
        }
        
        return SUCCESS;
    
    }

    int newRightBlk = BPlusTree::splitLeaf(blockNum, indices);

    if (newRightBlk == E_DISKFULL) {
         return E_DISKFULL;
    }
    
    int ret1 = SUCCESS, ret2 = SUCCESS;

    if(leafBlkHeader.pblock != -1) {
        
        InternalEntry entry;
        entry.attrVal = indices[MIDDLE_INDEX_LEAF].attrVal;
        entry.lChild = blockNum;
        entry.rChild = newRightBlk;
        
        ret1 = BPlusTree::insertIntoInternal(relId, attrName, leafBlkHeader.pblock, entry);
    
    } else {
    
        ret2 = BPlusTree::createNewRoot(relId, attrName, indices[MIDDLE_INDEX_LEAF].attrVal, blockNum, newRightBlk);

    }   

    if(ret1 < 0 || ret2 < 0) {
    
        return E_DISKFULL;
    
    }
    
    return SUCCESS;
}

int BPlusTree::splitLeaf(int leafBlockNum, Index indices[]) {

    IndLeaf rightBlk;
    IndLeaf leftBlk(leafBlockNum);

    int rightBlkNum = rightBlk.getBlockNum();

    int leftBlkNum = leftBlk.getBlockNum();

    if( rightBlkNum == E_DISKFULL) {
        return E_DISKFULL;
    }

    struct HeadInfo leftBlkHeader, rightBlkHeader;

    leftBlk.getHeader(&leftBlkHeader);
    rightBlk.getHeader(&rightBlkHeader);

    rightBlkHeader.numEntries = (MAX_KEYS_LEAF + 1) / 2;
    rightBlkHeader.pblock = leftBlkHeader.pblock;
    rightBlkHeader.lblock = leftBlkNum;
    rightBlkHeader.rblock = leftBlkHeader.rblock;
    rightBlk.setHeader(&rightBlkHeader);

    leftBlkHeader.numEntries = (MAX_KEYS_LEAF + 1) / 2;
    leftBlkHeader.rblock = rightBlkNum;
    leftBlk.setHeader(&leftBlkHeader);

    for(int i = 0; i < 32 ; ++i ){
        leftBlk.setEntry((void *) &indices[i], i);
    }

    for(int i = 0 ; i < 32 ; ++i ) {
        rightBlk.setEntry((void *) &indices[i+32], i);
    }

    return rightBlkNum;
}

int BPlusTree::insertIntoInternal(int relId, char attrName[ATTR_SIZE], int intBlockNum, InternalEntry intEntry) {

    AttrCatEntry attrCatEntry;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

    IndInternal intBlk(intBlockNum);
    
    struct HeadInfo blockHeader;
    intBlk.getHeader(&blockHeader);

    InternalEntry internalEntries[blockHeader.numEntries + 1];
    
    if(blockHeader.numEntries == 0) {

        internalEntries[0] = intEntry;

    } else {

        bool firstOccurence = false;
        int j = 0;
        int indexOfEntry = 0;

        for(int i = 0 ; i < blockHeader.numEntries ; ++i) {
            
            InternalEntry ithEntry;
            intBlk.getEntry((void *) &ithEntry, i);
            
            int cmpVal = compareAttrs(ithEntry.attrVal, intEntry.attrVal, attrCatEntry.attrType);

            if( cmpVal >= 0 && firstOccurence == false) {
                firstOccurence = true;
                internalEntries[j] = intEntry; 
                ++j;    
                internalEntries[j] = ithEntry;
                internalEntries[j].lChild = internalEntries[j-1].rChild;
                ++j;        
            }
            else {
                internalEntries[j] = ithEntry;
                if(j>=1) {
                    internalEntries[j].lChild = internalEntries[j-1].rChild;
                }
                ++j;
            }
        }    
        
        if(firstOccurence == false) {
            internalEntries[blockHeader.numEntries]  = intEntry;
            internalEntries[blockHeader.numEntries].lChild = internalEntries[blockHeader.numEntries-1].rChild;
        }    
    }

    if(blockHeader.numEntries != MAX_KEYS_INTERNAL) {
        
        blockHeader.numEntries++;
        intBlk.setHeader(&blockHeader);

        for(int i = 0 ; i < blockHeader.numEntries ; ++i) {
            intBlk.setEntry((void *) &internalEntries[i], i);
        }        

        return SUCCESS;
    }
    
    int newRightBlk = BPlusTree::splitInternal(intBlockNum, internalEntries);
    
    if(newRightBlk == E_DISKFULL) {
        
        BPlusTree::bPlusDestroy(intEntry.rChild);
        return E_DISKFULL;
    
    }

    int ret1 = SUCCESS, ret2 = SUCCESS;

    if(blockHeader.pblock != -1) {
    
        InternalEntry entry;

        entry.attrVal = internalEntries[MIDDLE_INDEX_INTERNAL].attrVal;
        entry.lChild = intBlockNum;
        entry.rChild = newRightBlk;

        ret1 = BPlusTree::insertIntoInternal(relId, attrName, blockHeader.pblock, entry);

    } else {
        
        ret2 = BPlusTree::createNewRoot(relId, attrName, internalEntries[MIDDLE_INDEX_INTERNAL].attrVal, intBlockNum, newRightBlk);
    
    }

    if(ret1 < 0 || ret2 < 0) {
        return E_DISKFULL;    
    }

    return SUCCESS;
}

int BPlusTree::splitInternal(int intBlockNum, InternalEntry internalEntries []) {
    
    IndInternal rightBlk;
    IndInternal leftBlk(intBlockNum);

    int rightBlkNum = rightBlk.getBlockNum();
    int leftBlkNum = leftBlk.getBlockNum();

    if(rightBlkNum == E_DISKFULL) {
        return E_DISKFULL;
    }

    struct HeadInfo leftBlkHeader, rightBlkHeader;

    leftBlk.getHeader(&leftBlkHeader);
    rightBlk.getHeader(&rightBlkHeader);

    rightBlkHeader.numEntries = MAX_KEYS_INTERNAL / 2 ;
    rightBlkHeader.pblock = leftBlkHeader.pblock;
    rightBlk.setHeader(&rightBlkHeader);

    leftBlkHeader.numEntries = MAX_KEYS_INTERNAL / 2 ;
    leftBlk.setHeader(&leftBlkHeader);

    for(int i = 0 ; i < 50 ; ++i) {
        leftBlk.setEntry((void *) &internalEntries[i], i);
    }

    for(int i = 51 ; i <= 100 ; ++i) {
        rightBlk.setEntry((void *) &internalEntries[i], i-51);
    }

    int type = StaticBuffer::getStaticBlockType(intBlockNum);

    for(int i = 0 ; i < rightBlkHeader.numEntries ; ++i) {
        
        struct InternalEntry entry;
        rightBlk.getEntry((void *) &entry, i);
        
        BlockBuffer lChildBlk(entry.lChild), rChildBlk(entry.rChild);
        
        struct HeadInfo lChildBlkHeader;
        struct HeadInfo rChildBlkHeader;

        lChildBlk.getHeader(&lChildBlkHeader);
        rChildBlk.getHeader(&rChildBlkHeader);

        lChildBlkHeader.pblock = rightBlkNum;
        rChildBlkHeader.pblock = rightBlkNum;

        lChildBlk.setHeader(&lChildBlkHeader);
        rChildBlk.setHeader(&rChildBlkHeader);
    }

    return rightBlkNum;
}

int BPlusTree::createNewRoot(int relId, char attrName[ATTR_SIZE], Attribute attrVal, int lChild, int rChild) {
    
    AttrCatEntry attrCatEntry;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);
    IndInternal newRootBlk;

    int newRootBlkNum = newRootBlk.getBlockNum();

    if (newRootBlkNum == E_DISKFULL) {
        BPlusTree::bPlusDestroy(rChild);
        return E_DISKFULL;
    }

    struct HeadInfo newRootBlkHeader;

    newRootBlk.getHeader(&newRootBlkHeader);
    newRootBlkHeader.numEntries = 1;
    newRootBlk.setHeader(&newRootBlkHeader);

    InternalEntry firstEntry;
    
    firstEntry.attrVal = attrVal;
    firstEntry.lChild = lChild;
    firstEntry.rChild = rChild;

    newRootBlk.setEntry((void *) &firstEntry, 0);
    
    BlockBuffer lChildBlk(lChild);
    BlockBuffer rChildBlk(rChild);

    struct HeadInfo lChildHeader, rChildHeader;

    lChildBlk.getHeader(&lChildHeader);
    rChildBlk.getHeader(&rChildHeader);

    lChildHeader.pblock = newRootBlkNum;
    rChildHeader.pblock = newRootBlkNum;

    lChildBlk.setHeader(&lChildHeader);
    rChildBlk.setHeader(&rChildHeader);

    attrCatEntry.rootBlock = newRootBlkNum;

    AttrCacheTable::setAttrCatEntry(relId, attrName, &attrCatEntry);

    return SUCCESS;
}
