#include "Frontend.h"

#include <cstring>
#include <iostream>

int Frontend::create_table(char relname[ATTR_SIZE], int no_attrs, char attributes[][ATTR_SIZE],
                           int type_attrs[]) {
  // Schema::createRel
  return Schema::createRel(relname, no_attrs, attributes, type_attrs);
}

int Frontend::drop_table(char relname[ATTR_SIZE]) {
  // Schema::deleteRel
  return Schema::deleteRel(relname);
}

int Frontend::open_table(char relname[ATTR_SIZE]) {
  // Schema::openRel
  return Schema::openRel(relname);
}

int Frontend::close_table(char relname[ATTR_SIZE]) {
  // Schema::closeRel
  return Schema::closeRel(relname);
}

int Frontend::alter_table_rename(char relname_from[ATTR_SIZE], char relname_to[ATTR_SIZE]) {
  // Schema::renameRel
  return Schema::renameRel(relname_from, relname_to);
}

int Frontend::alter_table_rename_column(char relname[ATTR_SIZE], char attrname_from[ATTR_SIZE],
                                        char attrname_to[ATTR_SIZE]) {
  // Schema::renameAttr
  return Schema::renameAttr(relname, attrname_from, attrname_to);
}

int Frontend::create_index(char relname[ATTR_SIZE], char attrname[ATTR_SIZE]) {
  // Schema::createIndex
  return Schema::createIndex(relname, attrname);
}

int Frontend::drop_index(char relname[ATTR_SIZE], char attrname[ATTR_SIZE]) {
  // Schema::dropIndex
  return Schema::dropIndex(relname, attrname);
}

int Frontend::insert_into_table_values(char relname[ATTR_SIZE], int attr_count, char attr_values[][ATTR_SIZE]) {
  // Algebra::insert
  return Algebra::insert(relname, attr_count, attr_values);
}

int Frontend::select_from_table(char relname_source[ATTR_SIZE], char relname_target[ATTR_SIZE]) {
  // Algebra::project
  return Algebra::project(relname_source, relname_target);
}

int Frontend::select_attrlist_from_table(char relname_source[ATTR_SIZE], char relname_target[ATTR_SIZE],
                                         int attr_count, char attr_list[][ATTR_SIZE]) {
  // Algebra::project
  return Algebra::project(relname_source, relname_target, attr_count, attr_list);
}

int Frontend::select_from_table_where(char relname_source[ATTR_SIZE], char relname_target[ATTR_SIZE],
                                      char attribute[ATTR_SIZE], int op, char value[ATTR_SIZE]) {
  // Algebra::select
  return Algebra::select(relname_source, relname_target, attribute, op, value);
}

int Frontend::select_attrlist_from_table_where(char relname_source[ATTR_SIZE], char relname_target[ATTR_SIZE],
                                               int attr_count, char attr_list[][ATTR_SIZE],
                                               char attribute[ATTR_SIZE], int op, char value[ATTR_SIZE]) {
  // Algebra::select + Algebra::project??
  char temp_relation[ATTR_SIZE];
  memset(temp_relation, '\0', sizeof(temp_relation));
  temp_relation[0] = '.';
  temp_relation[1] = 't';
  temp_relation[2] = 'e';
  temp_relation[3] = 'm';
  temp_relation[4] = 'p';
  
  int ret = Algebra::select(relname_source, temp_relation, attribute , op, value);

  if (ret != SUCCESS) {
      return ret;
  }

  int tempRelId = OpenRelTable::openRel(temp_relation);

  if(tempRelId < 0) {
      Schema::deleteRel(temp_relation);
      return tempRelId;
  }

  int project_ret = Algebra::project(temp_relation, relname_target, attr_count, attr_list);

  Schema::closeRel(temp_relation);
  Schema::deleteRel(temp_relation);

  if( project_ret != SUCCESS) {
      return project_ret;
  }
  
  return SUCCESS;  
}

int Frontend::select_from_join_where(char relname_source_one[ATTR_SIZE], char relname_source_two[ATTR_SIZE],
                                     char relname_target[ATTR_SIZE],
                                     char join_attr_one[ATTR_SIZE], char join_attr_two[ATTR_SIZE]) {
  return Algebra::join(relname_source_one, relname_source_two, relname_target, join_attr_one, join_attr_two);
}

int Frontend::select_attrlist_from_join_where(char relname_source_one[ATTR_SIZE], char relname_source_two[ATTR_SIZE],
                                              char relname_target[ATTR_SIZE],
                                              char join_attr_one[ATTR_SIZE], char join_attr_two[ATTR_SIZE],
                                              int attr_count, char attr_list[][ATTR_SIZE]) {
      // Algebra::join + project
      char tempRelation[ATTR_SIZE];
      memset(tempRelation, '\0', sizeof(tempRelation));
      tempRelation[0] = '.';
      tempRelation[1] = 't';
      tempRelation[2] = 'e';
      tempRelation[3] = 'm';
      tempRelation[4] = 'p';
      
      int ret = Algebra::join(relname_source_one, relname_source_two, tempRelation, join_attr_one, join_attr_two);
      
      if(ret < 0) {
          return ret;
      }

      int tempRelId = OpenRelTable::openRel(tempRelation);

      if(tempRelId < 0) {
        Schema::deleteRel(tempRelation);
        return tempRelId;
      }

      int projectRet = Algebra::project(tempRelation, relname_target, attr_count, attr_list);
      
      OpenRelTable::closeRel(tempRelId);
      Schema::deleteRel(tempRelation);
      
      if(projectRet < 0) {
        printf("Hello World");
        return projectRet;       
      }
      
      return SUCCESS;
}

int Frontend::custom_function(int argc, char argv[][ATTR_SIZE]) {
  // argc gives the size of the argv array
  // argv stores every token delimited by space and comma

  // implement whatever you desire

  return SUCCESS;
}