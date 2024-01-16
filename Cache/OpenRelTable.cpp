#include "OpenRelTable.h"
#include <cstdlib>
#include <cstring>
#include <stdio.h>
#include <iostream>

using namespace std;

OpenRelTableMetaInfo OpenRelTable::tableMetaInfo[MAX_OPEN];

OpenRelTable::OpenRelTable() {
    
    for( int i=0 ; i < MAX_OPEN ; ++i ) {
        RelCacheTable::relCache[i] = nullptr;
        AttrCacheTable::attrCache[i] = nullptr;
        tableMetaInfo[i].free = true;
    }

    RecBuffer relCatBlock(RELCAT_BLOCK);

    Attribute relCatRecord[RELCAT_NO_ATTRS];
    relCatBlock.getRecord(relCatRecord, RELCAT_SLOTNUM_FOR_RELCAT);

    struct RelCacheEntry relCacheEntry;
    RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry);
    relCacheEntry.recId.block = RELCAT_BLOCK;
    relCacheEntry.recId.slot = RELCAT_SLOTNUM_FOR_RELCAT;

    RelCacheTable::relCache[RELCAT_RELID] = (struct RelCacheEntry*) malloc(sizeof(struct RelCacheEntry));
    *(RelCacheTable::relCache[RELCAT_RELID]) = relCacheEntry;

    Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
    relCatBlock.getRecord(attrCatRecord, RELCAT_SLOTNUM_FOR_ATTRCAT);

    RelCacheTable::recordToRelCatEntry(attrCatRecord, &relCacheEntry.relCatEntry);

    relCacheEntry.recId.block = RELCAT_BLOCK;
    relCacheEntry.recId.slot = RELCAT_SLOTNUM_FOR_ATTRCAT;    

    RelCacheTable::relCache[ATTRCAT_RELID] = (struct RelCacheEntry*) malloc(sizeof(struct RelCacheEntry));
    *(RelCacheTable::relCache[ATTRCAT_RELID]) = relCacheEntry;

    RecBuffer attrCatBlock(ATTRCAT_BLOCK);
    
    // list of AttrCacheEntry (slots 0 to 5)
    struct AttrCacheEntry *listHead, *listTail;
    listHead = nullptr;
    listTail = nullptr;

    for( int i = 0 ; i < 6 ; ++i ) {
        if(listHead == nullptr){
            struct AttrCacheEntry *attrCacheEntry = nullptr;
            attrCacheEntry = (struct AttrCacheEntry*) malloc(sizeof(struct AttrCacheEntry));
            attrCatBlock.getRecord(attrCatRecord, i);
            AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &attrCacheEntry->attrCatEntry);
            attrCacheEntry->recId.block = ATTRCAT_BLOCK;
            attrCacheEntry->recId.slot = i;
            listHead = attrCacheEntry;
            listTail = listHead;
        } else {
            struct AttrCacheEntry *attrCacheEntry = nullptr;
            attrCacheEntry = (struct AttrCacheEntry*) malloc(sizeof(struct AttrCacheEntry));
            attrCatBlock.getRecord(attrCatRecord, i);
            AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &attrCacheEntry->attrCatEntry);
            attrCacheEntry->recId.block = ATTRCAT_BLOCK;
            attrCacheEntry->recId.slot = i;
            listTail->next = attrCacheEntry;
            listTail = listTail->next;
        }      
    }
    
    listTail->next=nullptr;
    AttrCacheTable::attrCache[RELCAT_RELID] = listHead;

    listHead = nullptr;
    listTail = nullptr;
    
    for( int i = 6 ; i < 12 ; ++i ) {
        if(listHead == nullptr) {
            struct AttrCacheEntry *attrCacheEntry = nullptr;
            attrCacheEntry = (struct AttrCacheEntry*) malloc(sizeof(struct AttrCacheEntry));
            attrCatBlock.getRecord(attrCatRecord, i);
            AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &attrCacheEntry->attrCatEntry);
            attrCacheEntry->recId.block = ATTRCAT_BLOCK;
            attrCacheEntry->recId.slot = i;
            listHead = attrCacheEntry;
            listTail = listHead;
        } else {
            struct AttrCacheEntry *attrCacheEntry = nullptr;
            attrCacheEntry = (struct AttrCacheEntry*) malloc(sizeof(struct AttrCacheEntry));
            attrCatBlock.getRecord(attrCatRecord, i);
            AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &attrCacheEntry->attrCatEntry);
            attrCacheEntry->recId.block = ATTRCAT_BLOCK;
            attrCacheEntry->recId.slot = i;
            listTail->next = attrCacheEntry;
            listTail = listTail->next;
        }
    }

    listTail->next = nullptr;
    AttrCacheTable::attrCache[ATTRCAT_RELID] = listHead;

    // Set up the Table Meta Information Entries
    tableMetaInfo[RELCAT_RELID].free = false;
    tableMetaInfo[ATTRCAT_RELID].free = false;
    strcpy(tableMetaInfo[RELCAT_RELID].relName, RELCAT_RELNAME);
    strcpy(tableMetaInfo[ATTRCAT_RELID].relName, ATTRCAT_RELNAME);

}

OpenRelTable::~OpenRelTable() {
    for(int i = 2 ; i < MAX_OPEN ; ++i){
        if(!tableMetaInfo[i].free) {
            OpenRelTable::closeRel(i);
        }
    }
    /*** Closing the Relation Cache Catalog Relations ***/

    char attrRelName[ATTR_SIZE];
    memset(attrRelName, '\0', sizeof(attrRelName));
    attrRelName[0]='R';
    attrRelName[1]='e';
    attrRelName[2]='l';
    attrRelName[3]='N';
    attrRelName[4]='a';
    attrRelName[5]='m';
    attrRelName[6]='e';

    if(RelCacheTable::relCache[ATTRCAT_RELID]->dirty == true) {
        
        Attribute record[RELCAT_NO_ATTRS];
        RelCacheTable::relCatEntryToRecord(&RelCacheTable::relCache[ATTRCAT_RELID]->relCatEntry, record);
        RelCacheTable::resetSearchIndex(ATTRCAT_RELID); 

        Attribute attr;
        strcpy(attr.sVal, ATTRCAT_RELNAME);
        
        RecId recId = BlockAccess::linearSearch(RELCAT_RELID, attrRelName, attr, EQ);

        RecBuffer relCatBlock(recId.block);
        relCatBlock.setRecord(record, recId.slot);

    }

    free(RelCacheTable::relCache[ATTRCAT_RELID]);
    
    if(RelCacheTable::relCache[RELCAT_RELID]->dirty == true) {

        Attribute record[RELCAT_NO_ATTRS];
        RelCacheTable::relCatEntryToRecord(&RelCacheTable::relCache[RELCAT_RELID]->relCatEntry, record);
        RelCacheTable::resetSearchIndex(RELCAT_RELID); 

        Attribute attr;
        strcpy(attr.sVal, RELCAT_RELNAME);
        
        RecId recId = BlockAccess::linearSearch(RELCAT_RELID, attrRelName, attr, EQ);

        RecBuffer relCatBlock(recId.block);
        relCatBlock.setRecord(record, recId.slot);

    }

    free(RelCacheTable::relCache[RELCAT_RELID]);
    
    struct AttrCacheEntry *listHead0 = AttrCacheTable::attrCache[RELCAT_RELID];
    struct AttrCacheEntry *listHead1 = AttrCacheTable::attrCache[ATTRCAT_RELID];
    
    while (listHead0 != nullptr && listHead1 != nullptr) {
        struct AttrCacheEntry *temp1 = listHead0;
        struct AttrCacheEntry *temp2 = listHead1;
        listHead0 = listHead0->next;
        listHead1 = listHead1->next;
        free(temp1);
        free(temp2);
    }
    
    // free all the memory that you allocated in the constructor
}

int OpenRelTable::getRelId(char relName[ATTR_SIZE]) {
    // if relname is RELCAT_RELNAME, return RELCAT_RELID
    // if relname is ATTRCAT_RELNAME, return ATTRCAT_RELID;
    for( int i = 0 ; i < MAX_OPEN ; ++i ) {
        if(strcmp(tableMetaInfo[i].relName, relName) == 0 && tableMetaInfo[i].free==false) {
            return i;
        }
    }

    return E_RELNOTOPEN;
}

int OpenRelTable::getFreeOpenRelTableEntry() {
    for(int i=2; i<MAX_OPEN; ++i){
        if(tableMetaInfo[i].free == true) {
            return i;
        }
    }
    return E_CACHEFULL;
}

int OpenRelTable::openRel(char relName[ATTR_SIZE]) {
    int relId = OpenRelTable::getRelId(relName);
    
    if(relId != E_RELNOTOPEN) {
        return relId;
    }

    int freeSlot = OpenRelTable::getFreeOpenRelTableEntry();
    
    if(freeSlot == E_CACHEFULL) {
        return E_CACHEFULL;
    }

    relId = freeSlot;

    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute relAttr;
    
    
    strcpy(relAttr.sVal, relName);

    char attrRelName[ATTR_SIZE];
    memset(attrRelName, '\0', sizeof(attrRelName));
    attrRelName[0]='R';
    attrRelName[1]='e';
    attrRelName[2]='l';
    attrRelName[3]='N';
    attrRelName[4]='a';
    attrRelName[5]='m';
    attrRelName[6]='e';

    RecId relCatRecId = BlockAccess::linearSearch(RELCAT_RELID, attrRelName, relAttr, EQ);

    if(relCatRecId.block == -1 and relCatRecId.slot == -1) {
        return E_RELNOTEXIST;
    }

    int block = relCatRecId.block;
    int slot = relCatRecId.slot;

    RecBuffer relCatBlock(block);
    Attribute record[RELCAT_NO_ATTRS];

    relCatBlock.getRecord(record, slot);
    struct RelCacheEntry relCacheEntry;
    RelCacheTable::recordToRelCatEntry(record, &relCacheEntry.relCatEntry);
    relCacheEntry.recId = relCatRecId;
    
    RelCacheTable::relCache[relId] = (struct RelCacheEntry *) malloc(sizeof(struct RelCacheEntry));
    *(RelCacheTable::relCache[relId]) = relCacheEntry;

    /*** Setting up Attribute Cache Entry ***/
    AttrCacheEntry *listHead, *listTail;
    listHead = nullptr;
    listTail = nullptr;
    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
    
    while (true) {
        
        RecId attrCatRecId;
        Attribute attr;
        
        strcpy(attr.sVal, relName);
        
        attrCatRecId = BlockAccess::linearSearch(ATTRCAT_RELID, attrRelName, attr, EQ);
        
        if(attrCatRecId.block == -1 && attrCatRecId.slot == -1) {
            break;
        }

        int block = attrCatRecId.block;
        int slot = attrCatRecId.slot;

        RecBuffer attrCatBlock(block);
        Attribute record[ATTRCAT_NO_ATTRS];
  
        attrCatBlock.getRecord(record, slot);

        struct AttrCacheEntry* attrCacheEntry = nullptr;
        attrCacheEntry = (struct AttrCacheEntry *) malloc(sizeof(struct AttrCacheEntry));
        AttrCacheTable::recordToAttrCatEntry(record, &attrCacheEntry->attrCatEntry);
        attrCacheEntry->recId = attrCatRecId;
  
        if(listHead == nullptr) {
            listHead = attrCacheEntry;
            listTail = listHead;
        } 
        else {
            listTail->next = attrCacheEntry;
            listTail = listTail->next;
        }
    }

    listTail->next = nullptr;

    AttrCacheTable::attrCache[relId] = listHead; 

    OpenRelTable::tableMetaInfo[relId].free = false;
    strcpy(OpenRelTable::tableMetaInfo[relId].relName, relName);

    return relId;
}

int OpenRelTable::closeRel(int relId) {

    if(relId == RELCAT_RELID or relId == ATTRCAT_RELID) {
        return E_NOTPERMITTED;
    }

    if( relId >= MAX_OPEN && relId < 2 ) {
        return E_OUTOFBOUND;
    }

    if (tableMetaInfo[relId].free == true) {
        return E_RELNOTOPEN;
    }

    char attrRelName[ATTR_SIZE];
    memset(attrRelName, '\0', sizeof(attrRelName));
    attrRelName[0]='R';
    attrRelName[1]='e';
    attrRelName[2]='l';
    attrRelName[3]='N';
    attrRelName[4]='a';
    attrRelName[5]='m';
    attrRelName[6]='e';
    
    if( RelCacheTable::relCache[relId]->dirty == true ) {
        union Attribute record[RELCAT_NO_ATTRS];

        struct RelCatEntry relCatEntry;
        relCatEntry = RelCacheTable::relCache[relId]->relCatEntry;

        RelCacheTable::relCatEntryToRecord(&relCatEntry, record);
        
        RecId recId = RelCacheTable::relCache[relId]->recId;
        RecBuffer relCatBlock(recId.block);
        relCatBlock.setRecord(record, recId.slot);
    }    
    
    free(RelCacheTable::relCache[relId]);

    RelCacheTable::relCache[relId] = nullptr;

    struct AttrCacheEntry *listHead = AttrCacheTable::attrCache[relId];
    
    while (listHead != nullptr) {
        
        RecId recId = listHead->recId;

        RecBuffer recordBlk(recId.block);
        Attribute record[ATTRCAT_NO_ATTRS];
        AttrCacheTable::attrCatEntryToRecord(&listHead->attrCatEntry, record);
        recordBlk.setRecord(record, recId.slot);
    
        struct AttrCacheEntry *temp = listHead;
        listHead = listHead->next;
        free(temp);
    
    }
    
    AttrCacheTable::attrCache[relId] = nullptr;
    OpenRelTable::tableMetaInfo[relId].free = true;

    return SUCCESS;
}