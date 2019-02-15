#ifndef CONT_CAGG_H
#define CONT_CAGG_H

void cagg_validate_query(Query *query);
void cagg_create(ViewStmt *stmt, Query *panquery);

#endif
