#include "BlockAccess.h"
#include <cstring>
#include <cstdio>
#include <iostream>


using namespace std;

RecId BlockAccess::linearSearch(int relId, char attrName[ATTR_SIZE], union Attribute attrVal, int op) {
    
    RecId prevRecId;
    RelCacheTable::getSearchIndex(relId, &prevRecId);
    
    int block = -1 , slot = -1;

    if(prevRecId.block == -1 and prevRecId.slot == -1) {
    
        struct RelCatEntry relCatEntry;
        RelCacheTable::getRelCatEntry(relId, &relCatEntry);
        block = relCatEntry.firstBlk;
        slot = 0;
    
    } else{
    
        block = prevRecId.block;
        slot = prevRecId.slot+1;
    
    }

    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(relId, &relCatEntry);
    
    int numAttrs = relCatEntry.numAttrs;
    
    while(block != -1) {
        
        struct RecBuffer blockBuff(block);
        Attribute record[numAttrs];
    
        struct HeadInfo blockHeader;

        blockBuff.getRecord(record, slot);
        blockBuff.getHeader(&blockHeader);

        unsigned char * slotMap = nullptr;

        slotMap = (unsigned char *) malloc(blockHeader.numSlots * sizeof(unsigned char));

        blockBuff.getSlotMap(slotMap);
        
        if(slot >= blockHeader.numSlots) {
            block = blockHeader.rblock;
            slot = 0;
            free(slotMap);
            continue;
        }

        if(slotMap[slot] == SLOT_UNOCCUPIED) {
            ++slot;
            free(slotMap);
            continue;
        }
        
        free(slotMap);

        AttrCatEntry attrCatEntry;
        
        AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);
        
        int offset = attrCatEntry.offset;

        Attribute value = record[offset];

        int cmpVal = compareAttrs(value, attrVal, attrCatEntry.attrType);

        if(
            (op == NE && cmpVal != 0) ||
            (op == LT && cmpVal < 0) ||
            (op == LE && cmpVal <= 0) ||
            (op == EQ && cmpVal == 0) ||
            (op == GT && cmpVal > 0) ||
            (op == GE && cmpVal >= 0)
        ) {
            RecId recId;
            recId.block = block;
            recId.slot = slot;
            RelCacheTable::setSearchIndex(relId, &recId);
            return RecId{block, slot};
        }
        slot++;
    
    }

    return RecId{-1,-1};

}

int BlockAccess::renameRelation(char oldName[ATTR_SIZE], char newName[ATTR_SIZE]) {

    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute newRelationName;
    strcpy(newRelationName.sVal, newName);
    
    char attrRelName[ATTR_SIZE];
    memset(attrRelName, '\0', sizeof(attrRelName));
    attrRelName[0]='R';
    attrRelName[1]='e';
    attrRelName[2]='l';
    attrRelName[3]='N';
    attrRelName[4]='a';
    attrRelName[5]='m';
    attrRelName[6]='e';
    
    RecId searchIndex = BlockAccess::linearSearch(RELCAT_RELID, attrRelName, newRelationName, EQ);

    if (searchIndex.block != -1 && searchIndex.slot != -1) {
        return E_RELEXIST;
    }

    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute oldRelationName;
    strcpy(oldRelationName.sVal, oldName);

    searchIndex = BlockAccess::linearSearch(RELCAT_RELID, attrRelName, oldRelationName, EQ);

    if(searchIndex.block == -1 && searchIndex.slot == -1) {
        return E_RELNOTEXIST;
    }

    int block = searchIndex.block;
    int slot = searchIndex.slot;

    RecBuffer relCatBlock(block);

    Attribute record[RELCAT_NO_ATTRS];
    struct HeadInfo relCatHeader;

    relCatBlock.getRecord(record, slot);
    relCatBlock.getHeader(&relCatHeader);

    strcpy(record[RELCAT_REL_NAME_INDEX].sVal, newName);
    relCatBlock.setRecord(record, slot);

    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

    for (int i = 0 ; i < relCatHeader.numAttrs ; ++i) {
        RecId recId = BlockAccess::linearSearch(ATTRCAT_RELID, attrRelName, oldRelationName, EQ);
        
        if( recId.block != -1 && recId.slot != -1 ) {
            
            RecBuffer attrCatBlock(recId.block);
            Attribute record[ATTRCAT_NO_ATTRS];
            
            attrCatBlock.getRecord(record, recId.slot);
            strcpy(record[ATTRCAT_REL_NAME_INDEX].sVal, newName);
            attrCatBlock.setRecord(record, recId.slot);
        
        }
    
    }

    return SUCCESS;
}


int BlockAccess::renameAttribute(char relName[ATTR_SIZE], char oldName[ATTR_SIZE], char newName[ATTR_SIZE]) {
    
    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute relNameAttr;
    strcpy(relNameAttr.sVal, relName);

    char attrRelName[ATTR_SIZE];
    memset(attrRelName, '\0', sizeof(attrRelName));
    attrRelName[0]='R';
    attrRelName[1]='e';
    attrRelName[2]='l';
    attrRelName[3]='N';
    attrRelName[4]='a';
    attrRelName[5]='m';
    attrRelName[6]='e';
    
    
    RecId relSearchIndex = BlockAccess::linearSearch(RELCAT_RELID, attrRelName, relNameAttr, EQ);

    if(relSearchIndex.block == -1 && relSearchIndex.slot == -1) {
        return E_RELNOTEXIST;
    }

    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

    RecId attrToRenameRecId {-1,-1};
    Attribute record[ATTRCAT_NO_ATTRS];

    while(true) {

        RecId searchIndex = BlockAccess::linearSearch(ATTRCAT_RELID, attrRelName, relNameAttr, EQ);
        
        int block = searchIndex.block;
        int slot = searchIndex.slot;
        
        if(block == -1 || slot == -1) {
            break;
        }
        
        RecBuffer attrCatBlock(block);

        Attribute attrCatEntryRecord[ATTRCAT_NO_ATTRS];
        attrCatBlock.getRecord(attrCatEntryRecord, slot);
        
        if( strcmp(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, oldName) == 0 ) {
            attrToRenameRecId = searchIndex;
        }

        if( strcmp(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, newName) == 0 ) {
            return E_ATTREXIST;
        }

    }

    if(attrToRenameRecId.block == -1 && attrToRenameRecId.slot == -1) {
        return E_ATTRNOTEXIST;
    }

    RecBuffer renameBlock(attrToRenameRecId.block);

    renameBlock.getRecord(record, attrToRenameRecId.slot);

    strcpy(record[ATTRCAT_ATTR_NAME_INDEX].sVal, newName);

    renameBlock.setRecord(record, attrToRenameRecId.slot);

    return SUCCESS;
}

int BlockAccess::insert(int relId, union Attribute * record) {
    
    struct RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(relId, &relCatEntry);

    int blockNum = relCatEntry.firstBlk;

    RecId rec_id = {-1, -1};

    int numOfSlots = relCatEntry.numSlotsPerBlk;
    int numOfAttributes = relCatEntry.numAttrs;
    int prevBlockNum = -1;

    while (blockNum != -1) {
        
        RecBuffer recordBlock(blockNum);
        struct HeadInfo blockHeader;

        recordBlock.getHeader(&blockHeader);
        
        unsigned char * slotMap = nullptr;

        slotMap = (unsigned char *) malloc(blockHeader.numSlots * sizeof(unsigned char));

        recordBlock.getSlotMap(slotMap);
        
        int freeSlotIndex = -1;
    
        for(int slotIndex = 0 ; slotIndex < blockHeader.numSlots ; ++slotIndex) {
            if(slotMap[slotIndex] == SLOT_UNOCCUPIED) {
                freeSlotIndex = slotIndex;
                break;
            }
        }

        if(freeSlotIndex != -1) {    
            rec_id.block = blockNum;
            rec_id.slot = freeSlotIndex;
            break;
        } else{
            prevBlockNum = blockNum;
            blockNum = blockHeader.rblock;
        }
    }

    if(rec_id.block == -1 && rec_id.slot == -1) {
        
        if(relId == RELCAT_RELID) {
            return E_MAXRELATIONS;
        }

        RecBuffer newRecordBlock;
        int ret = newRecordBlock.getBlockNum();

        if(ret == E_DISKFULL) {
            return E_DISKFULL;
        }

        rec_id.block = ret;
        rec_id.slot = 0;

        struct HeadInfo blockHeader;
        
        blockHeader.blockType = REC;
        blockHeader.pblock = -1;
        blockHeader.lblock = prevBlockNum;
        blockHeader.rblock = -1;
        blockHeader.numEntries = 0;
        blockHeader.numSlots = numOfSlots;
        blockHeader.numAttrs = numOfAttributes;

        newRecordBlock.setHeader(&blockHeader);

        
        unsigned char * newSlotMap = nullptr;

        newSlotMap = (unsigned char *) malloc(blockHeader.numSlots * sizeof(unsigned char));

        memset(newSlotMap, SLOT_UNOCCUPIED, numOfSlots);

        newRecordBlock.setSlotMap(newSlotMap);

        if(prevBlockNum != -1) {
            
            RecBuffer prevBlock(prevBlockNum);

            struct HeadInfo prevBlockHeader;
            prevBlock.getHeader(&prevBlockHeader);

            prevBlockHeader.rblock = rec_id.block;
            prevBlock.setHeader(&prevBlockHeader);

        } else{
            
            relCatEntry.firstBlk = rec_id.block;
            RelCacheTable::setRelCatEntry(relId, &relCatEntry);  
        
        }

        relCatEntry.lastBlk = rec_id.block;
        RelCacheTable::setRelCatEntry(relId, &relCatEntry);
    }

    RecBuffer recordBlock(rec_id.block);
    recordBlock.setRecord(record, rec_id.slot);

    unsigned char * slotMap = nullptr;

    slotMap = (unsigned char *) malloc(numOfSlots * sizeof(unsigned char));

    recordBlock.getSlotMap(slotMap);
    
    slotMap[rec_id.slot] = SLOT_OCCUPIED;

    recordBlock.setSlotMap(slotMap);

    free(slotMap);

    struct HeadInfo recordBlockHeader;
    
    recordBlock.getHeader(&recordBlockHeader);
    
    recordBlockHeader.numEntries++;
    
    recordBlock.setHeader(&recordBlockHeader);

    relCatEntry.numRecs++;

    RelCacheTable::setRelCatEntry(relId, &relCatEntry);
    int flag = SUCCESS;

    for(int attrOffset = 0 ; attrOffset < relCatEntry.numAttrs ; ++attrOffset) {

        AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(relId, attrOffset, &attrCatEntry);

        int rootBlk = attrCatEntry.rootBlock;

        if( rootBlk != -1) {
            int retVal = BPlusTree::bPlusInsert(relId, attrCatEntry.attrName, record[attrOffset], rec_id);

            if(retVal == E_DISKFULL) {
                flag = E_INDEX_BLOCKS_RELEASED;
            }
        }
    }

    return flag;
}   

int BlockAccess::search(int relId, Attribute * record, char attrName[ATTR_SIZE], Attribute attrVal, int op) {

    RecId recId;

    struct AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

    if( ret != SUCCESS) {
        return ret;
    }

    int rootBlock = attrCatEntry.rootBlock;
    
    if(rootBlock == -1) {
        recId = BlockAccess::linearSearch(relId, attrName, attrVal, op);
    } else {
        recId = BPlusTree::bPlusSearch(relId, attrName, attrVal, op);
    }

    if(recId.block == -1 && recId.slot == -1) {
        return E_NOTFOUND;
    }

    RecBuffer recordBlock (recId.block);
    recordBlock.getRecord(record, recId.slot);

    return SUCCESS;
}

int BlockAccess::deleteRelation(char relName[ATTR_SIZE]) {
    
    if(strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0) {
        return E_NOTPERMITTED;
    }

    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    char attrRelName[ATTR_SIZE];
    memset(attrRelName, '\0', sizeof(attrRelName));
    attrRelName[0]='R';
    attrRelName[1]='e';
    attrRelName[2]='l';
    attrRelName[3]='N';
    attrRelName[4]='a';
    attrRelName[5]='m';
    attrRelName[6]='e';
    
    Attribute relNameAttr;
    strcpy(relNameAttr.sVal, relName);

    RecId relCatSearchIndex = BlockAccess::linearSearch(RELCAT_RELID, attrRelName, relNameAttr, EQ);

    if(relCatSearchIndex.block == -1 && relCatSearchIndex.slot == -1) {
        return E_RELNOTEXIST;
    }

    Attribute relCatEntryRecord[RELCAT_NO_ATTRS];

    RecBuffer relCatBlock(relCatSearchIndex.block);

    relCatBlock.getRecord(relCatEntryRecord, relCatSearchIndex.slot);

    int firstBlock = relCatEntryRecord[RELCAT_FIRST_BLOCK_INDEX].nVal;
    int numAttrs = relCatEntryRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal;

    int currBlock = firstBlock;

    while(currBlock != -1) {

        RecBuffer currBuff(currBlock);
        
        struct HeadInfo currHead;
        currBuff.getHeader(&currHead);

        currBlock = currHead.rblock;    
        currBuff.releaseBlock();
    
    }

    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

    int numberOfAttributesDeleted = 0;
    
    int relId = OpenRelTable::getRelId(relName);

    while (true) {

        RecId attrCatRecId;

        attrCatRecId = BlockAccess::linearSearch(ATTRCAT_RELID, attrRelName, relNameAttr, EQ);

        if(attrCatRecId.block == -1 && attrCatRecId.slot == -1) {
            break;
        }

        numberOfAttributesDeleted++;

        RecBuffer attrCatBlock(attrCatRecId.block);

        struct HeadInfo attrCatHeader;
        attrCatBlock.getHeader(&attrCatHeader);

        Attribute record[ATTRCAT_NO_ATTRS];
        attrCatBlock.getRecord(record, attrCatRecId.slot);

        int rootBlock = record[ATTRCAT_ROOT_BLOCK_INDEX].nVal;
        int numSlots = attrCatHeader.numSlots;

        unsigned char * slotMap = nullptr;

        slotMap = (unsigned char *) malloc(numSlots * sizeof(unsigned char));

        attrCatBlock.getSlotMap(slotMap);
        slotMap[attrCatRecId.slot] = SLOT_UNOCCUPIED;
        attrCatBlock.setSlotMap(slotMap);

        free(slotMap);

        attrCatHeader.numEntries--;

        attrCatBlock.setHeader(&attrCatHeader);

        if( attrCatHeader.numEntries == 0) {
            
            RecBuffer leftBlock(attrCatHeader.lblock);
            struct HeadInfo leftBlockHeader, rightBlockHeader;

            leftBlock.getHeader(&leftBlockHeader);

            if(attrCatHeader.rblock != -1) {
                RecBuffer rightBlock(attrCatHeader.rblock);
                rightBlock.getHeader(&rightBlockHeader);
                rightBlockHeader.lblock = attrCatHeader.lblock;
                rightBlock.setHeader(&rightBlockHeader);
            } 
            else {
                RelCatEntry relCatEntry;
                RelCacheTable::getRelCatEntry(RELCAT_RELID, &relCatEntry);
                relCatEntry.lastBlk = attrCatHeader.lblock;
                RelCacheTable::setRelCatEntry(RELCAT_RELID, &relCatEntry);
            }

            attrCatBlock.releaseBlock();
        
        }

        if(rootBlock != -1) {
            BPlusTree::bPlusDestroy(rootBlock);
        }

    }
    
    struct HeadInfo relCatHeader;
    
    relCatBlock.getHeader(&relCatHeader);
    
    relCatHeader.numEntries--;
    
    relCatBlock.setHeader(&relCatHeader);

    unsigned char* slotMap  = nullptr;

    slotMap = (unsigned char *) malloc(relCatHeader.numSlots * sizeof(unsigned char));

    
    relCatBlock.getSlotMap(slotMap);
    
    slotMap[relCatSearchIndex.slot]  = SLOT_UNOCCUPIED;
    
    relCatBlock.setSlotMap(slotMap);

    free(slotMap);

    struct RelCatEntry relCatEntry;
    
    RelCacheTable::getRelCatEntry(RELCAT_RELID, &relCatEntry);
    relCatEntry.numRecs--;
    RelCacheTable::setRelCatEntry(RELCAT_RELID, &relCatEntry);

    RelCatEntry attrCatEntry;
    
    RelCacheTable::getRelCatEntry(ATTRCAT_RELID, &attrCatEntry);
    attrCatEntry.numRecs -= numberOfAttributesDeleted;
    RelCacheTable::setRelCatEntry(ATTRCAT_RELID, &attrCatEntry);

    return SUCCESS;
}

int BlockAccess::project(int relId, Attribute * record) {
    
    RecId prevRecId;
    RelCacheTable::getSearchIndex(relId, &prevRecId);
    
    int block, slot;

    if(prevRecId.block == -1 && prevRecId.slot == -1) {
    
        struct RelCatEntry relCatEntry;
        RelCacheTable::getRelCatEntry(relId, &relCatEntry);
    
        block = relCatEntry.firstBlk;
        slot = 0;
    
    } 
    else {
        block = prevRecId.block;
        slot = prevRecId.slot + 1;
    }

    while(block != -1) {
    
        RecBuffer recordBlock(block);
    
        struct HeadInfo blockHead;
    
        recordBlock.getHeader(&blockHead);

        unsigned char * slotMap = nullptr;

        slotMap = (unsigned char *) malloc(blockHead.numSlots * sizeof(unsigned char));

        recordBlock.getSlotMap(slotMap);
    
        if(slot >= blockHead.numSlots) {
            block = blockHead.rblock;
            slot = 0;
        } else if( slotMap[slot] == SLOT_UNOCCUPIED ) {
            ++slot;
        } else {
            break;
        }

        free(slotMap);        
    
    }

    if(block == -1) {
        return E_NOTFOUND;
    }

    RecId nextRecId {block, slot};

    RelCacheTable::setSearchIndex(relId, &nextRecId);
    // set the search index to nextRecId using RelCacheTable::setSearchIndex

    RecBuffer targetRecBlock(nextRecId.block);
    targetRecBlock.getRecord(record, nextRecId.slot);

    return SUCCESS;
}