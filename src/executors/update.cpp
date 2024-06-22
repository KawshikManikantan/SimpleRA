#include "global.h"
/**
 * @brief 
 * SYNTAX: R <- SELECT column_name bin_op [column_name | int_literal] FROM relation_name
 */
bool syntacticParseUPDATE()
{
    logger.log("syntacticParseSELECTION");
    if (tokenizedQuery.size() != 6 || tokenizedQuery[2] != "COLUMN")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = UPDATE;
    parsedQuery.updateTableName = tokenizedQuery[1];
    parsedQuery.updateColumnName = tokenizedQuery[3];
    parsedQuery.updateOperator = tokenizedQuery[4];
    parsedQuery.updateValue = tokenizedQuery[5];

    string binaryOperator = tokenizedQuery[4];
    if (binaryOperator == "MULTIPLY" ||binaryOperator == "ADD"|| binaryOperator == "SUBTRACT" );
    else
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    return true;
}

bool semanticParseUPDATE()
{
    logger.log("semanticParseSELECTION");


    if (!tableCatalogue.isTable(parsedQuery.updateTableName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }

    if (!tableCatalogue.isColumnFromTable(parsedQuery.updateColumnName, parsedQuery.updateTableName))
    {
        cout << "SEMANTIC ERROR: Column doesn't exist in relation" << endl;
        return false;
    }
    return true;
}

void evaluateOperation(int& field,int value)
{
    if(parsedQuery.updateOperator=="MULTIPLY")
        field*=value;
    else if(parsedQuery.updateOperator=="ADD")
        field+=value;
    else if(parsedQuery.updateOperator=="SUBTRACT")
        field-=value;
}

void executeUPDATE()
{
    
    logger.log("executeSELECTION");
    //lockManager.update_start_file();
    //while(lockManager.ping_start_file() != 2);
    bufferManager.clearPool();
    while(lockManager.status_of_table(parsedQuery.updateTableName) != 0){
        cout<<"Update blocked"<<endl;
    }
    lockManager.change_lock_status(parsedQuery.updateTableName,2);
    Table* table = tableCatalogue.getTable(parsedQuery.updateTableName);
    int ColumnIndex = table->getColumnIndex(parsedQuery.updateColumnName);
    Page page = bufferManager.getPage(table->tableName,0);
    int numrows = page.getsize();
    for(int i=0;i<numrows;i++)
    {
        vector<int> row = page.getRow(i);
        int value1 = row[ColumnIndex];
        evaluateOperation(value1,stoi(parsedQuery.updateValue));
        page.modIndexOfRow(i,ColumnIndex,value1);
        sleep(1);
    }
    page.writePage();
    bufferManager.clearPool();
    table->makePermanent();
    lockManager.change_lock_status(parsedQuery.updateTableName,0);
    return;
}
