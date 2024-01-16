#include "Buffer/StaticBuffer.h"
#include "Cache/OpenRelTable.h"
#include "Disk_Class/Disk.h"
#include "FrontendInterface/FrontendInterface.h"

int main(int argc, char *argv[]) {
  /* Initialize the Run Copy of Disk */
  // StaticBuffer buffer;
  // OpenRelTable cache;
  Disk disk_run;
  StaticBuffer buffer;
  OpenRelTable cache;
    // RecBuffer relCatBuffer(RELCAT_BLOCK);
  // RecBuffer attrCatBuffer(ATTRCAT_BLOCK);
  
  // HeadInfo relCatHeader;
  // HeadInfo attrCatHeader;

  // relCatBuffer.getHeader(&relCatHeader);
  // attrCatBuffer.getHeader(&attrCatHeader);
  /*
    relCatBuffer.getHeader(&relCatHeader);
    for(int i=0 ;  i < relCatHeader.numEntries ; ++i) {
          Attribute relCatRecord[RELCAT_NO_ATTRS];
          relCatBuffer.getRecord(relCatRecord, i);

          printf("Relation: %s\n", relCatRecord[RELCAT_REL_NAME_INDEX].sVal);
          HeadInfo attrCatHeader;  
          int currentBlock = ATTRCAT_BLOCK;
          
          while (currentBlock != -1)
          {    
              RecBuffer attrCatBuffer(currentBlock);
              attrCatBuffer.getHeader(&attrCatHeader);
              for( int j=0; j < attrCatHeader.numEntries ; ++j ) {
                  Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
                  attrCatBuffer.getRecord(attrCatRecord, j);
                  if(strcmp(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal,relCatRecord[RELCAT_REL_NAME_INDEX].sVal)==0) {
                      const char *attrType = attrCatRecord[ATTRCAT_ATTR_TYPE_INDEX].nVal == NUMBER ? "NUM": "STR";
                      printf("%s: %s\n", attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, attrType);
                  }
              }
              printf("\n");
              currentBlock = attrCatHeader.rblock;
          }
    }
  */
  /*
    for(int i=0 ;  i < relCatHeader.numEntries ; ++i) {
        Attribute relCatRecord[RELCAT_NO_ATTRS];
        relCatBuffer.getRecord(relCatRecord, i);

        printf("Relation: %s\n", relCatRecord[RELCAT_REL_NAME_INDEX].sVal);

        for( int j=0; j < attrCatHeader.numEntries; ++j ) {
            Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
            attrCatBuffer.getRecord(attrCatRecord, j);
            if(strcmp(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal,relCatRecord[RELCAT_REL_NAME_INDEX].sVal)==0) {
                const char *attrType = attrCatRecord[ATTRCAT_ATTR_TYPE_INDEX].nVal == NUMBER ? "NUM": "STR";
                printf(" %s: %s\n", attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, attrType);
            }
        }
        printf("\n");
    }
  */
  /*
    for( int i = 0 ; i < 2 ; ++i ) {
        struct RelCatEntry relCatBuf;
        RelCacheTable::getRelCatEntry(i, &relCatBuf);
        printf("Relation: %s\n", relCatBuf.relName);
        for(int j = 0 ; j < relCatBuf.numAttrs ; ++j) {
            struct AttrCatEntry attrCatBuf;
            AttrCacheTable::getAttrCatEntry(i, j,&attrCatBuf);
            const char *attrType = attrCatBuf.attrType == NUMBER ? "NUM": "STR";
            printf(" %s: %s\n", attrCatBuf.attrName, attrType);
        }
    }
  */
  return FrontendInterface::handleFrontend(argc, argv);
}