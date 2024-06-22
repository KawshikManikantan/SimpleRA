
class LockManager{
    string lockfilename = "../data/temp/lock";
    public:
    int num_elements = 0;
    int start = -1;
    string startfilename = "../data/temp/start";
    vector<string> lock_tables;
    vector<int> lock_status;
    LockManager();
    void read_lock_file();
    void write_lock_file();
    void create_lock_file();
    void change_lock_status(string tableName,int lock_type);
    void insert_table_to_lock(string tableName);
    int status_of_table(string tableName);
    void update_start_file();
    void create_start_file();
    int ping_start_file();
    bool isFileExists(string name);
};