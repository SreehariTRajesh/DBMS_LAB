#include "AttrCacheTable.h"
#include <cstring>
#include <cstdio>

AttrCacheEntry* AttrCacheTable::attrCache[MAX_OPEN];

int AttrCacheTable::getAttrCatEntry(int relId, int attrOffset, AttrCatEntry* attrCatBuf) {
    
    if(!(relId >=0 and relId <MAX_OPEN)) {
        return E_OUTOFBOUND;
    }

    if(attrCache[relId] == nullptr) {
        return E_RELNOTOPEN;
    }

    for(AttrCacheEntry *entry = attrCache[relId]; entry != nullptr ; entry = entry->next) {
        if(entry->attrCatEntry.offset == attrOffset) {
            *attrCatBuf = entry->attrCatEntry;
            return SUCCESS;
        }
    }

    return E_ATTRNOTEXIST;
}

void AttrCacheTable::recordToAttrCatEntry(union Attribute record[ATTRCAT_NO_ATTRS],
                                          AttrCatEntry* attrCatEntry) {
                                            
    strcpy(attrCatEntry->relName, record[ATTRCAT_REL_NAME_INDEX].sVal);
    strcpy(attrCatEntry->attrName, record[ATTRCAT_ATTR_NAME_INDEX].sVal);
    attrCatEntry->attrType = (int) record[ATTRCAT_ATTR_TYPE_INDEX].nVal;
    attrCatEntry->rootBlock = (int) record[ATTRCAT_ROOT_BLOCK_INDEX].nVal;
    attrCatEntry->offset = (int) record[ATTRCAT_OFFSET_INDEX].nVal;
    attrCatEntry->primaryFlag = (int) record[ATTRCAT_PRIMARY_FLAG_INDEX].nVal;
    // copy the rest of the fields in the record to the attrCacheEntry struct
}


int AttrCacheTable::getAttrCatEntry(int relId, char attrName[ATTR_SIZE], AttrCatEntry* attrCatBuf) {
    if(relId < 0 || relId >= MAX_OPEN) {
        return E_OUTOFBOUND;
    }

    if(attrCache[relId] == nullptr) {
        return E_RELNOTOPEN;
    }

    for(AttrCacheEntry *entry = attrCache[relId]; entry != nullptr ; entry = entry->next) {
        if(strcmp(entry->attrCatEntry.attrName, attrName)==0) {
            *attrCatBuf = entry->attrCatEntry;
            return SUCCESS;
        }
    }
    return E_ATTRNOTEXIST;
}

int AttrCacheTable::getSearchIndex(int relId, char attrName[ATTR_SIZE], IndexId *searchIndex) {
    
    if(relId < 0 || relId >= MAX_OPEN) {
        return E_OUTOFBOUND;
    }

    if(attrCache[relId] == nullptr) {
        return E_RELNOTOPEN;
    }

    for(AttrCacheEntry *entry = attrCache[relId]; entry != nullptr ; entry = entry->next) {
        if(strcmp(entry->attrCatEntry.attrName, attrName) == 0) {
            *searchIndex = entry->searchIndex;
            return SUCCESS;
        }
    }

    return E_ATTRNOTEXIST;
}

int AttrCacheTable::getSearchIndex(int relId, int attrOffset, IndexId *searchIndex) {
    
    if(relId < 0 || relId >= MAX_OPEN) {
        return E_OUTOFBOUND;
    }

    if(attrCache[relId] == nullptr) {
        return E_RELNOTOPEN;
    }

    for(AttrCacheEntry *entry = attrCache[relId]; entry != nullptr ; entry = entry->next) {
        if(entry->attrCatEntry.offset== attrOffset) {
            *searchIndex = entry->searchIndex;
            return SUCCESS;
        }
    }

    return E_ATTRNOTEXIST;
}

int AttrCacheTable::setSearchIndex(int relId, char attrName[ATTR_SIZE], IndexId *searchIndex) {
    
    if(relId < 0 || relId >= MAX_OPEN) {
        return E_OUTOFBOUND;
    }

    if(attrCache[relId] == nullptr) {
        return E_RELNOTOPEN;
    }

    for(AttrCacheEntry *entry = attrCache[relId]; entry != nullptr ; entry = entry->next) {
        if(strcmp(entry->attrCatEntry.attrName, attrName) == 0) {
            entry->searchIndex = *searchIndex;
            return SUCCESS;
        }
    }

    return E_ATTRNOTEXIST;

}

int AttrCacheTable::setSearchIndex(int relId, int attrOffset, IndexId *searchIndex) {
    
    if(relId < 0 || relId >= MAX_OPEN) {
        return E_OUTOFBOUND;
    }

    if(attrCache[relId] == nullptr) {
        return E_RELNOTOPEN;
    }

    for(AttrCacheEntry *entry = attrCache[relId]; entry != nullptr ; entry = entry->next) {
        if(entry->attrCatEntry.offset== attrOffset) {
            entry->searchIndex = *searchIndex;
            return SUCCESS;
        }
    }

    return E_ATTRNOTEXIST;
}

int AttrCacheTable::resetSearchIndex(int relId, char attrName[ATTR_SIZE]) {
    IndexId searchIndex{-1, -1};
    int ret = AttrCacheTable::setSearchIndex(relId, attrName, &searchIndex);
    return ret;
}


int AttrCacheTable::resetSearchIndex(int relId, int attrOffset) {
    IndexId searchIndex{-1, -1};
    int ret = AttrCacheTable::setSearchIndex(relId, attrOffset, &searchIndex);
    return ret;
}

int AttrCacheTable::setAttrCatEntry(int relId, char attrName[ATTR_SIZE], AttrCatEntry * attrCatBuf) {

    if( relId < 0 || relId >= MAX_OPEN) {
        return E_OUTOFBOUND;
    }

    if(AttrCacheTable::attrCache[relId] == nullptr) {
        return E_RELNOTOPEN;
    }

    for(AttrCacheEntry* entry = AttrCacheTable::attrCache[relId]; entry != nullptr ; entry = entry->next) {
        if(strcmp(entry->attrCatEntry.attrName, attrName) == 0) {
            entry->attrCatEntry = *attrCatBuf;
            return SUCCESS;
        }
    }

    return E_ATTRNOTEXIST;
}

int AttrCacheTable::setAttrCatEntry(int relId, int attrOffset, AttrCatEntry * attrCatBuf) {

    if( relId < 0 || relId >= MAX_OPEN) {
        return E_OUTOFBOUND;
    }

    if(AttrCacheTable::attrCache[relId] == nullptr) {
        return E_RELNOTOPEN;
    }

    for(AttrCacheEntry* entry = AttrCacheTable::attrCache[relId]; entry != nullptr ; entry = entry->next) {
        if(entry->attrCatEntry.offset == attrOffset) {
            entry->attrCatEntry = *attrCatBuf;
            return SUCCESS;
        }
    }

    return E_ATTRNOTEXIST;
}

void AttrCacheTable::attrCatEntryToRecord(AttrCatEntry * attrCatEntry,  Attribute * record) {
    strcpy(record[ATTRCAT_ATTR_NAME_INDEX].sVal, attrCatEntry->attrName);
    strcpy(record[ATTRCAT_REL_NAME_INDEX].sVal, attrCatEntry->relName);
    record[ATTRCAT_ATTR_TYPE_INDEX].nVal = attrCatEntry->attrType;
    record[ATTRCAT_OFFSET_INDEX].nVal = attrCatEntry->offset;
    record[ATTRCAT_PRIMARY_FLAG_INDEX].nVal = attrCatEntry->primaryFlag;
    record[ATTRCAT_ROOT_BLOCK_INDEX].nVal = attrCatEntry->rootBlock;
}
