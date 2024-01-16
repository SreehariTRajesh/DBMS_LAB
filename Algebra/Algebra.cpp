#include "Algebra.h"
#include "../Cache/AttrCacheTable.h"
#include "../Buffer/BlockBuffer.h"
#include <cstring>
#include <cstdlib>
#include <cstdio>


bool isNumber(char *str) {
  int len;
  float ignore;
  int ret = sscanf(str, "%f %n", &ignore, &len);
  return ret == 1 && len == strlen(str);
}

int Algebra::select(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], char attr[ATTR_SIZE], int op, char strVal[ATTR_SIZE]) {
    
    int srcRelId = OpenRelTable::getRelId(srcRel);
    
    if(srcRelId == E_RELNOTOPEN) {
        return E_RELNOTOPEN;
    }
    
    AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getAttrCatEntry(srcRelId, attr, &attrCatEntry);

    if(ret != SUCCESS) {
        return E_ATTRNOTEXIST;
    }

    int type = attrCatEntry.attrType;
    Attribute attrVal;

    if(type == NUMBER) {
        if(isNumber(strVal)) {
            attrVal.nVal = atof(strVal);
        } else {
            return E_ATTRTYPEMISMATCH;
        }
    } else if(type == STRING) {
        strcpy(attrVal.sVal, strVal);
    }
    /*** Creating and Opening the Target Relation ***/
    // Prepare arguments for createRel() in the following way:
    
    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(srcRelId, &relCatEntry);    

    
    if(strcmp(targetRel, "null") == 0) {

        printf("|");
        for (int i = 0; i < relCatEntry.numAttrs; ++i) {
            AttrCatEntry attrCatEntry;
            AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatEntry);
            printf(" %s |", attrCatEntry.attrName);
        }
        printf("\n");
        
        Attribute record[relCatEntry.numAttrs];  
        RelCacheTable::resetSearchIndex(srcRelId);  
        AttrCacheTable::resetSearchIndex(srcRelId, attr);

        while (BlockAccess::search(srcRelId, record, attr, attrVal, op) == SUCCESS) {
            
            printf("|");
            for(int i=0; i< relCatEntry.numAttrs ; ++i) {
                AttrCatEntry attrCatEntry;
                AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatEntry);                
                if(attrCatEntry.attrType == STRING) {
                    printf(" %s |", record[i].sVal);
                } else if(attrCatEntry.attrType == NUMBER) {
                    printf(" %d |", int(record[i].nVal));
                }
            }
        
            printf("\n");
        
        }

        return SUCCESS;

    }

    int src_nAttrs = relCatEntry.numAttrs;

    char attr_names[src_nAttrs][ATTR_SIZE];
    int attr_types[src_nAttrs];
    
    for(int i = 0; i < src_nAttrs ; ++i) {
        AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatEntry);
        strcpy(attr_names[i], attrCatEntry.attrName);
        attr_types[i] = attrCatEntry.attrType;
    }

    int retVal = Schema::createRel(targetRel, src_nAttrs, attr_names, attr_types);
    
    if( retVal != SUCCESS) {
         return retVal;
    }

    int targetRelId = OpenRelTable::openRel(targetRel);

    if(targetRelId < 0) {
        Schema::deleteRel(targetRel);
        return E_CACHEFULL;
    }

    Attribute record[src_nAttrs];
    RelCacheTable::resetSearchIndex(srcRelId);
    AttrCacheTable::resetSearchIndex(srcRelId, attr);
        
    while (BlockAccess::search(srcRelId, record, attr, attrVal, op) == SUCCESS)
    {     
        int retVal = BlockAccess::insert(targetRelId, record);
    
        if(retVal != SUCCESS) {
            Schema::closeRel(targetRel);
            Schema::deleteRel(targetRel);
            return retVal;
        }
    }
    
    Schema::closeRel(targetRel);    
    
    return SUCCESS;
}


int Algebra::insert(char relName[ATTR_SIZE], int nAttrs, char record[][ATTR_SIZE]) {

    if(strcmp(relName, RELCAT_RELNAME) == 0 && strcmp(relName, ATTRCAT_RELNAME) == 0) {
        return E_NOTPERMITTED;
    }

    int relId = OpenRelTable::getRelId(relName);

    if (relId == E_RELNOTOPEN) {
        return E_RELNOTOPEN;
    }

    struct RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(relId, &relCatEntry);

    if(relCatEntry.numAttrs != nAttrs) {
        return E_NATTRMISMATCH;
    }

    union Attribute recordValues[relCatEntry.numAttrs];

    for(int attrIndex=0 ; attrIndex < nAttrs ; ++attrIndex) {
        struct AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(relId, attrIndex, &attrCatEntry);

        int type = attrCatEntry.attrType;

        if(type == NUMBER) {
            if(isNumber(record[attrIndex]) == true) {
                recordValues[attrIndex].nVal = atof(record[attrIndex]);
            }
            else {
                return E_ATTRTYPEMISMATCH;
            }
        } 
        else if(type == STRING) {
            strcpy(recordValues[attrIndex].sVal, record[attrIndex]);
        }
    }

    int retVal = BlockAccess::insert(relId, recordValues);
    
    return retVal;
}


int Algebra::project(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE]) {
    
    int srcRelId = OpenRelTable::getRelId(srcRel);

    if(srcRelId == E_RELNOTOPEN) {
        return E_RELNOTOPEN;
    }

    struct RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(srcRelId, &relCatEntry);

    int numAttrs = relCatEntry.numAttrs;

    char attrNames[numAttrs][ATTR_SIZE];
    int attrTypes[numAttrs];

    for(int i = 0 ; i < numAttrs ; ++i) {
        struct AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatEntry);
        strcpy(attrNames[i], attrCatEntry.attrName);
        attrTypes[i] = attrCatEntry.attrType;
    }
    
    if(strcmp(targetRel, "null") == 0) {
        printf("|");
        for(int i = 0 ; i < numAttrs ; ++i) {
            struct AttrCatEntry attrCatEntry;
            AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatEntry);
            printf(" %s |", attrCatEntry.attrName);
        }
        printf("\n");
        RelCacheTable::resetSearchIndex(srcRelId);
        Attribute record[numAttrs];

        while (BlockAccess::project(srcRelId, record) == SUCCESS) 
        {
            printf("|");
            for(int i = 0 ; i < numAttrs ; ++i ){
                struct AttrCatEntry attrCatEntry;
                AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatEntry);

                if(attrCatEntry.attrType == STRING) {
                    printf(" %s |", record[i].sVal);
                } else if(attrCatEntry.attrType == NUMBER) {
                    printf(" %d |", (int) record[i].nVal);
                }   
            }
            printf("\n");
            /* code */
        }
        return SUCCESS;
    }

    /*** Creating and Opening a Target Relation ***/
    int ret = Schema::createRel(targetRel, numAttrs, attrNames, attrTypes);
    
    if(ret != SUCCESS) {
        return ret;
    }

    int targetRelId = OpenRelTable::openRel(targetRel);

    // open fails;

    if(targetRelId < 0) {
        Schema::deleteRel(targetRel);
        return targetRelId;
    }

    /*** Inserting projected records into the target relation ***/
    RelCacheTable::resetSearchIndex(srcRelId);
    
    Attribute record[numAttrs];

    while (BlockAccess::project(srcRelId, record) == SUCCESS) {
        int ret = BlockAccess::insert(targetRelId, record);
        if( ret < 0) {
            Schema::closeRel(targetRel);
            Schema::deleteRel(targetRel);
            return ret;
        }
    }
 
    Schema::closeRel(targetRel);

    return SUCCESS;
}

int Algebra::project(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], int tar_nAttrs, char tar_Attrs[][ATTR_SIZE]) {
    
    int srcRelId = OpenRelTable::getRelId(srcRel);

    if(srcRelId == E_RELNOTOPEN) {
        return E_RELNOTOPEN;
    }

    struct RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(srcRelId, &relCatEntry);

    if(strcmp(targetRel, "null") == 0) {
        
        printf("|");
        for(int i = 0 ; i < tar_nAttrs ; ++i) {
            for(int j = 0 ; j < relCatEntry.numAttrs ; ++j) {
                struct AttrCatEntry attrCatEntry;
                AttrCacheTable::getAttrCatEntry(srcRelId, j, &attrCatEntry);
                if(strcmp(tar_Attrs[i], attrCatEntry.attrName) == 0) {
                    printf("%s |", tar_Attrs[i]);
                }    
            }
        }
        printf("\n");

        RelCacheTable::resetSearchIndex(srcRelId);
        Attribute record[relCatEntry.numAttrs];

        while(BlockAccess::project(srcRelId, record) == SUCCESS) {
            printf("|");
            for( int i = 0 ; i < tar_nAttrs ; ++i ){
                for(int j = 0 ; j < relCatEntry.numAttrs ; ++j) {
                    struct AttrCatEntry attrCatEntry;
                    AttrCacheTable::getAttrCatEntry(srcRelId, j, &attrCatEntry);
                    if(strcmp(tar_Attrs[i], attrCatEntry.attrName) == 0) {
                        if(attrCatEntry.attrType == STRING) {
                            printf(" %s |", record[j].sVal);
                        } else if (attrCatEntry.attrType == NUMBER) {
                            printf(" %d |", (int)record[j].nVal);
                        }
                    }
                }
            }
            printf("\n");
        }

        return SUCCESS;
    }


    int src_nAttrs = relCatEntry.numAttrs;
    int attr_offset[tar_nAttrs];
    int attr_types[tar_nAttrs];

    for(int i = 0 ; i < tar_nAttrs ; ++i) {
        AttrCatEntry attrCatEntry;
        int ret = AttrCacheTable::getAttrCatEntry(srcRelId, tar_Attrs[i], &attrCatEntry);
        if( ret == E_ATTRNOTEXIST) {
            return E_ATTRNOTEXIST;
        }
        attr_offset[i] = attrCatEntry.offset;
        attr_types[i] = attrCatEntry.attrType;
    }

    int ret = Schema::createRel(targetRel, tar_nAttrs, tar_Attrs, attr_types);

    if( ret != SUCCESS) {
        return ret;
    }

    // Open the relation for target relation by calling OpenRelTable::openRel
    int targetRelId = OpenRelTable::openRel(targetRel);

    if( targetRelId < 0 ) {
        Schema::deleteRel(targetRel);
        return targetRelId;
    }

    RelCacheTable::resetSearchIndex(srcRelId);

    Attribute record[src_nAttrs];

    while ( BlockAccess::project(srcRelId, record) == SUCCESS ) {
        
        Attribute proj_record[tar_nAttrs];

        for (int i = 0 ; i < tar_nAttrs ; ++i) {
            proj_record[i] = record[attr_offset[i]];      
        }

        int ret = BlockAccess::insert(targetRelId, proj_record);

        if(ret != SUCCESS) {
            Schema::closeRel(targetRel);
            Schema::deleteRel(targetRel);
            return ret;
        }
    }
    
    Schema::closeRel(targetRel);

    return SUCCESS;
}

int Algebra::join(char srcRelation1[ATTR_SIZE], char srcRelation2[ATTR_SIZE], char targetRelation[ATTR_SIZE], char attribute1[ATTR_SIZE], char attribute2[ATTR_SIZE]) {

    int srcRelId1 = OpenRelTable::getRelId(srcRelation1);
    int srcRelId2 = OpenRelTable::getRelId(srcRelation2);

    if( srcRelId1 == E_RELNOTOPEN || srcRelId2 == E_RELNOTOPEN) {
        return E_RELNOTOPEN;
    }

    AttrCatEntry attrCatEntry1, attrCatEntry2;

    int ret1 = AttrCacheTable::getAttrCatEntry(srcRelId1, attribute1, &attrCatEntry1);
    int ret2 = AttrCacheTable::getAttrCatEntry(srcRelId2, attribute2, &attrCatEntry2);

    if(ret1 == E_ATTRNOTEXIST || ret2 == E_ATTRNOTEXIST) {
        return E_ATTRNOTEXIST;
    }

    if(attrCatEntry1.attrType != attrCatEntry2.attrType) {
        return E_ATTRTYPEMISMATCH;
    }
    
    RelCatEntry relCatEntry1, relCatEntry2;

    RelCacheTable::getRelCatEntry(srcRelId1, &relCatEntry1);
    RelCacheTable::getRelCatEntry(srcRelId2, &relCatEntry2);

    int numOfAttributes1 = relCatEntry1.numAttrs;
    int numOfAttributes2 = relCatEntry2.numAttrs;

    int attrCount1 = 0;
    
    for(int offset = 0 ; offset < numOfAttributes1 ; ++offset) {
        
        AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(srcRelId1, offset, &attrCatEntry);

        if( strcmp(attrCatEntry.attrName, attribute1) == 0 ) {
            ++attrCount1;
        }

    }

    int attrCount2 = 0;

    for(int offset = 0 ; offset < numOfAttributes2 ; ++offset) {
        
        AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(srcRelId2, offset, &attrCatEntry);

        if( strcmp(attrCatEntry.attrName, attribute2) == 0 ) {
            ++attrCount2;
        }
   
    }
    
    if(attrCount2 > 1 || attrCount1 > 1) {

        return E_DUPLICATEATTR;
    }

    if(attrCatEntry2.rootBlock == -1) {
        
        int ret = BPlusTree::bPlusCreate(srcRelId2, attribute2);
        
        if(ret == E_DISKFULL) {
            
            return ret;
        
        }
    
    }
    
    int numOfAttributesInTarget = numOfAttributes1 + numOfAttributes2 - 1;

    char targetRelAttrNames[numOfAttributesInTarget][ATTR_SIZE];
    int targetRelAttrTypes[numOfAttributesInTarget];

    int targetAttrIndex = 0;
    for(int i = 0 ; i < numOfAttributes1 ; ++i) {
    
        AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(srcRelId1, i, &attrCatEntry);
        strcpy(targetRelAttrNames[targetAttrIndex], attrCatEntry.attrName);
        targetRelAttrTypes[targetAttrIndex] = attrCatEntry.attrType;
        ++targetAttrIndex;
    
    }

    for(int i = 0 ; i < numOfAttributes2 ; ++i) {
        AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(srcRelId2, i, &attrCatEntry);
        if( strcmp(attribute2, attrCatEntry.attrName) != 0 ) {
            strcpy(targetRelAttrNames[targetAttrIndex], attrCatEntry.attrName);
            targetRelAttrTypes[targetAttrIndex] = attrCatEntry.attrType;
            ++targetAttrIndex;
        }
    }
    
    int ret = Schema::createRel(targetRelation, numOfAttributesInTarget, targetRelAttrNames, targetRelAttrTypes);

    if(ret < 0) {

        return ret;
    
    }

    int targetRelId = OpenRelTable::openRel(targetRelation);

    if(targetRelId < 0) {
    
        Schema::deleteRel(targetRelation);
        return targetRelId;
    
    }

    Attribute record1[numOfAttributes1];
    Attribute record2[numOfAttributes2];
    Attribute targetRecord[numOfAttributesInTarget];

    RelCacheTable::resetSearchIndex(srcRelId1);

    while(BlockAccess::project(srcRelId1, record1) == SUCCESS) {

        RelCacheTable::resetSearchIndex(srcRelId2);
        AttrCacheTable::resetSearchIndex(srcRelId2, attribute2);

        while (BlockAccess::search(srcRelId2, record2, attribute2, record1[attrCatEntry1.offset], EQ) == SUCCESS) {
            
            int targetOffset = 0;
            
            for(int offset = 0; offset < numOfAttributes1 ; ++offset) {
            
                targetRecord[targetOffset] = record1[offset];
                ++targetOffset;
            
            }

            for(int offset = 0; offset < numOfAttributes2 ; ++offset) {
            
                AttrCatEntry attrCatEntry;
                AttrCacheTable::getAttrCatEntry(srcRelId2, offset, &attrCatEntry);
            
                if(strcmp(attrCatEntry.attrName, attribute2) != 0) {
            
                    targetRecord[targetOffset] = record2[offset];
                    ++targetOffset;
            
                }
            
            }

            int insertStatus = BlockAccess::insert(targetRelId, targetRecord);

            if(insertStatus == E_DISKFULL) {
                OpenRelTable::closeRel(targetRelId);
                Schema::deleteRel(targetRelation);
                return E_DISKFULL;
            }

        }
    }

    OpenRelTable::closeRel(targetRelId);

    return SUCCESS;
}