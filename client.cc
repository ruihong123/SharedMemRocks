#include <assert.h>
#include "rocksdb/db.h"
#include <string>
#include <iostream>
int main()
{
  rocksdb::DB* db;
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::Status status =
      rocksdb::DB::Open(options, "/tmp/testdb", &db);
  assert(status.ok());
  std::string value;
  std::string key;
  rocksdb::Status s = db->Put(rocksdb::WriteOptions(), "StartKey", "StartValue");
  for (int i = 0; i<1000000; i++){
    key = std::to_string(i);
    value = std::to_string(i);
    if (s.ok()) s = db->Put(rocksdb::WriteOptions(), key, value);

  }
  s = db->Get(rocksdb::ReadOptions(), "50", &value);
  if(s.ok()) std::cout<< value << std::endl;
  return 0;
}
