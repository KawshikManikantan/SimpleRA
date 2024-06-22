#include "global.h"

LockManager::LockManager()
{
    logger.log("LockManager::LockManager");
}

void LockManager::create_lock_file()
{
    if(!(this->isFileExists(this->lockfilename)))
    {
        ofstream fout(this->lockfilename);
        fout.close();
    }
    // fout<<"a"<<" "<<0<<endl;
    // this->num_elements = 1;
    // this->read_lock_file();
}

void LockManager::read_lock_file()
{
    bool flag=0;
    while(flag==0 && this->num_elements != 0)
    {
        ifstream fin(this->lockfilename, ios::in);
        this->lock_tables.clear();
        this->lock_status.clear();
        //cout<<"Here"<<endl;
        string a,b;
        for(int i=0;i<this->num_elements;i++)
        {
            fin >> a >> b;
            //cout<<"Lock status is "<<a<<" "<<b<<" "<<a.length()<<" "<<b.length()<<endl;
            if(a.length()!=0 && b.length()!=0)
            {
                this->lock_tables.push_back(a);
                this->lock_status.push_back(stoi(b));
                flag=1;
            }
            else
                flag=0;
        }
        fin.close();
    }
}

void LockManager::write_lock_file()
{
    ofstream fout(this->lockfilename,ios::trunc);
    for(int i=0;i<this->num_elements;i++)
    {
        fout<< this->lock_tables[i]<<" "<<this->lock_status[i]<<endl;
    }
    fout.close();
}

void LockManager::change_lock_status(string tableName,int lock_type)
{
    this->read_lock_file();
    /*for(int i=0;i<this->lock_tables.size();i++)
        cout<<this->lock_tables[i]<<" ";
    cout<<endl;*/
    //cout<<tableName<<endl;
    auto it = find((this->lock_tables).begin(),(this->lock_tables).end(),tableName);
    if (it == this->lock_tables.end())
    {
        cout<<"Trying to change status of a table not inserted"<<endl;
        exit(1);
    }
    else
    {
        int ind = it - (this->lock_tables).begin();
        //cout<<"Status of lock before is "<<this->lock_status[ind]<<endl;
        this->lock_status[ind] = lock_type;
        //cout<<"Status of lock afterwards is "<<this->lock_status[ind]<<endl;
    }
    this->write_lock_file();
}

void LockManager::insert_table_to_lock(string tableName)
{
    this->read_lock_file();
    auto it = find((this->lock_tables).begin(),(this->lock_tables).end(),tableName);
    if (it == this->lock_tables.end())
    {
        this->lock_tables.push_back(tableName);
        this->lock_status.push_back(0);
        this->num_elements++;
        this->write_lock_file();
    }
}

int LockManager::status_of_table(string tableName)
{
    this->read_lock_file();
    auto it = find((this->lock_tables).begin(),(this->lock_tables).end(),tableName);
    int status = -1;
    /*cout<<"Tables inserted "<<endl;
    for(int i=0;i<this->lock_tables.size();i++)
        cout<<this->lock_tables[i]<<" ";
    cout<<endl;*/
    //cout<<tableName<<endl;
    if (it == this->lock_tables.end())
    {
        cout<<"Trying to find status of a table not inserted"<<endl;
        exit(1);
    }
    else
    {
        int ind = it - (this->lock_tables).begin();
        status = this->lock_status[ind];
    }
    return status;
}

void LockManager::update_start_file()
{
    ping_start_file();
    //cout<<this->start<<endl;
    this->start += 1;
   // cout<<"Writing this start into file"<<endl;
    ofstream fout(this->startfilename);
    fout<<this->start<<endl;
    fout.close();
}

int LockManager::ping_start_file()
{
    ifstream fin(this->lockfilename, ios::in);
    string s;
    fin >> s;
    //this->start=stoi(s);
    //cout<<"Pinged "<<s<<endl;
    fin.close();
    return this->start;
}

bool LockManager:: isFileExists(string name)
{
    struct stat buffer;
    return (stat(name.c_str(), &buffer) == 0);
}

void read_start_file()
{

}
void LockManager::create_start_file()
{
    if(!(this->isFileExists(this->startfilename)))
    {
        ofstream fout(this->startfilename);
        fout<<0<<endl;
        fout.close();
    }
}