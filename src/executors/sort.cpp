#include"global.h"
/**
 * @brief File contains method to process SORT commands.
 * 
 * syntax:
 * R <- SORT relation_name BY column_name IN sorting_order
 * 
 * sorting_order = ASC | DESC 
 */
bool syntacticParseSORT(){
    logger.log("syntacticParseSORT");
    if((tokenizedQuery.size()!= 10 && tokenizedQuery.size()!= 8) || tokenizedQuery[4] != "BY" || tokenizedQuery[6] != "IN" ){
        cout<<"SYNTAX ERROR"<<endl;
        return false;
    }
    if(tokenizedQuery.size()== 10 && tokenizedQuery[8]!="BUFFER")
    {
        cout<<"SYNTAX ERROR"<<endl;
        return false;
    }
    parsedQuery.queryType = SORT;
    parsedQuery.sortResultRelationName = tokenizedQuery[0];
    parsedQuery.sortRelationName = tokenizedQuery[3];
    parsedQuery.sortColumnName = tokenizedQuery[5];
    string sortingStrategy = tokenizedQuery[7];
    parsedQuery.sortBufferSize = stoi(tokenizedQuery[9]);

    if(sortingStrategy == "ASC")
        parsedQuery.sortingStrategy = ASC;
    else if(sortingStrategy == "DESC")
        parsedQuery.sortingStrategy = DESC;
    else{
        cout<<"SYNTAX ERROR"<<endl;
        return false;
    }
    return true;
}

bool semanticParseSORT(){
    logger.log("semanticParseSORT");

    if(tableCatalogue.isTable(parsedQuery.sortResultRelationName)){
        cout<<"SEMANTIC ERROR: Resultant relation already exists"<<endl;
        return false;
    }

    if(!tableCatalogue.isTable(parsedQuery.sortRelationName)){
        cout<<"SEMANTIC ERROR: Relation doesn't exist"<<endl;
        return false;
    }

    if(!tableCatalogue.isColumnFromTable(parsedQuery.sortColumnName, parsedQuery.sortRelationName)){
        cout<<"SEMANTIC ERROR: Column doesn't exist in relation"<<endl;
        return false;
    }

    return true;
}

bool sortcol(const vector<int>& v1, const vector<int>& v2)
{
    if(parsedQuery.sortingStrategy == ASC)
        return v1[parsedQuery.sortcolIdx] <= v2[parsedQuery.sortcolIdx];
    else
        return v1[parsedQuery.sortcolIdx] >= v2[parsedQuery.sortcolIdx];
}

// bool comp(const pair<int,int>& v1, const pair<int,int>& v2)
// {
//     if(parsedQuery.sortingStrategy == ASC)
//         return v1.first <= v2.first;
//     //else
//     return v1.first >= v2.first;
// }

class comp{
    public:
    bool operator()(pair<int,int>& v1, pair<int,int>& v2)
    {
        if(parsedQuery.sortingStrategy == ASC)
            return v1.first >= v2.first;
        else
        // cout<<v1.first<<" "<<v2.first<<endl;
            return v1.first <= v2.first;
    }
};

vector<int> initial_sort(Table table)
{
    int bufferSize = parsedQuery.sortBufferSize-1;
    vector<int>rowcounts;
    // Load Pages into Buffer
    for(int i=0;i<table.blockCount;)
    {
        vector<Page> pages;
        for(int j=0;j<bufferSize;j++)
        {
            if(i+j >= table.blockCount) break;
            pages.push_back(bufferManager.getPage(table.tableName, i+j));
        }
        
        for(int j=0;j<pages.size();j++)
        {
            vector<vector<int>> rows=pages[j].getallRows();
            int numrows = pages[j].getsize();
            sort(rows.begin(), rows.begin() + numrows, sortcol);
            //cout<<"Writing Page: "<<i+j<<endl;
            rowcounts.push_back(numrows);
            bufferManager.writePage(table.tableName + "_0",i+j,rows,numrows);
        }
        i=i+pages.size();
    }
    return rowcounts;
}

void executeSORT(){
    logger.log("executeSORT");
    Table rel  = *tableCatalogue.getTable(parsedQuery.sortRelationName);
    Table* resultantTable  = new Table(parsedQuery.sortResultRelationName,rel.columns);
    parsedQuery.sortcolIdx = rel.getColumnIndex(parsedQuery.sortColumnName);
    vector<int> rowcounts = initial_sort(rel);

    int itr = 0;
    int block_count = rel.blockCount;
    int limit = 1;
    int sort_round = 1;
    int bufferSizeSort = parsedQuery.sortBufferSize -1;
    int maxrows = rel.maxRowsPerBlock;

    while(itr != 1)
    {
        itr = 0;
        int pages_written = 0;

        for(int round = 0;round<block_count;round += pow(bufferSizeSort,sort_round))
        {
            itr += 1;
            vector<Page> pages;
            vector<int> rowcounts_buffer; // Maximum number of rows of the page loaded in buffer
            vector<int> curr_page_buffer; // How many pages of the sorted partition is read till now. // Starts from 0 should not exceed limit where limit is size of each sorted partition
            vector<int> curr_row_buffer;  // The row of the page in the priority queue
            //std::function<bool(Foo, Foo)>> pq(Compare)
            //priority_queue<pair<int,int>,vector<pair<int,int>,function<bool(pair<int,int>,pair<int,int>)>>> sort_queue(comp);//ascending or descending
            priority_queue<pair<int,int>,vector<pair<int,int>>,comp> sort_queue;//ascending or descending
            vector<vector<int>> rows_write; // Rows to write in new sorted iteration
            vector<int>initial_pages_loaded; // First page of each buffer location

            int num_pages = 0;
            
            //cout<<"Pages Loaded:"<<endl;
            for(int buffer_loader=round;buffer_loader<block_count;buffer_loader+=limit)
            {
                pages.push_back(bufferManager.getPartition(rel.tableName + "_" + to_string(sort_round -1), buffer_loader,rel.columnCount,rowcounts[buffer_loader]));
                rowcounts_buffer.push_back(rowcounts[buffer_loader]);
                curr_page_buffer.push_back(0);
                curr_row_buffer.push_back(0);
                initial_pages_loaded.push_back(buffer_loader);
                vector<int> row = pages[pages.size()-1].getRow(0);
                sort_queue.push(make_pair(row[parsedQuery.sortcolIdx],pages.size()-1));
                //cout<<buffer_loader<<endl;
                //cout<<"Pushed: "<< row[parsedQuery.sortcolIdx] << " "<< pages.size()-1 <<endl;
                num_pages += 1;
                
                if(num_pages >= bufferSizeSort)
                {
                    break;
                }
                // curr_page = buffer_loader;
            }
            
            while(!sort_queue.empty())
            {
                int value = sort_queue.top().first;
                int position = sort_queue.top().second;
                //cout<<"Value: "<<value<<" Position: "<<position<<endl;
                sort_queue.pop();

                //Write popped row to file
                vector<int> row_to_write = pages[position].getRow(curr_row_buffer[position]);
                rows_write.push_back(row_to_write);
                if(rows_write.size() >= maxrows)
                {
                    bufferManager.writePage(rel.tableName + "_" + to_string(sort_round),pages_written,rows_write,rows_write.size());
                    pages_written += 1;
                    rows_write.clear();
                }
                
                // Replace with next row if possible
                curr_row_buffer[position]++;
                int fail = 0;
                
                if(curr_row_buffer[position] >= rowcounts_buffer[position])
                {
                    curr_page_buffer[position] += 1;
                    if(curr_page_buffer[position] >= limit || (curr_page_buffer[position] + initial_pages_loaded[position] >= block_count))
                    {
                        fail = 1;
                    }
                    else
                    {
                        int page_to_load = initial_pages_loaded[position] + curr_page_buffer[position];
                        pages[position] = bufferManager.getPartition(rel.tableName + "_" + to_string(sort_round -1) , page_to_load,rel.columnCount,rowcounts[page_to_load]);
                        curr_row_buffer[position] = 0;
                        rowcounts_buffer[position] = rowcounts[page_to_load];
                        // curr_page = page_to_load;
                    }
                }

                if(fail == 0)
                {
                    vector<int> row = pages[position].getRow(curr_row_buffer[position]);
                    sort_queue.push(make_pair(row[parsedQuery.sortcolIdx],position));
                    //cout<<"Pushed: "<< row[parsedQuery.sortcolIdx] << " "<< position <<endl;
                } 
            }

            if(rows_write.size() > 0)
            {
                bufferManager.writePage(rel.tableName + "_" + to_string(sort_round),pages_written,rows_write,rows_write.size());
                pages_written += 1;
                rows_write.clear();
            }


        }
        limit = pow(bufferSizeSort,sort_round);
        sort_round += 1;
        // break;
    }

    for(int i=0;i<block_count;i++)
    {
        Page page = bufferManager.getPartition(rel.tableName + "_" + to_string(sort_round -1), i,rel.columnCount,rowcounts[i]);
        vector<vector<int>> rows_result = page.getallRows();
        bufferManager.writePage(resultantTable->tableName,i,rows_result,rows_result.size());
        resultantTable->rowsPerBlockCount.push_back(rows_result.size());
        resultantTable->blockCount++;
        resultantTable->rowCount += rows_result.size();
    }
    if(resultantTable->rowCount>0)
    {
        //cout<<"Inserting the table "<<endl;
        tableCatalogue.insertTable(resultantTable);
    }

    return;
}