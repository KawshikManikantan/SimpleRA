#include "global.h"
/**
 * @brief 
 * SYNTAX: PRINT relation_name
 */
bool syntacticParsePRINT()
{
    logger.log("syntacticParsePRINT");
    if (tokenizedQuery.size() != 2)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = PRINT;
    parsedQuery.printRelationName = tokenizedQuery[1];
    return true;
}

bool semanticParsePRINT()
{
    logger.log("semanticParsePRINT");
    if (!tableCatalogue.isTable(parsedQuery.printRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }
    return true;
}

void executePRINT()
{
    logger.log("executePRINT");
    //cout<<" Updating start file for print ";
    //lockManager.update_start_file();
    //cout<<" Updated start file for print ";
    //while(lockManager.ping_start_file() != 2)
    //{}
    bufferManager.clearPool();
    cout<<lockManager.status_of_table(parsedQuery.printRelationName)<<endl;
    while(lockManager.status_of_table(parsedQuery.printRelationName) == 2)
    {
        cout<<"Print blocked."<<endl;
    }

    lockManager.change_lock_status(parsedQuery.printRelationName,1);
    Table* table = tableCatalogue.getTable(parsedQuery.printRelationName);
    table->print();
    lockManager.change_lock_status(parsedQuery.printRelationName,0);
    return;
}
