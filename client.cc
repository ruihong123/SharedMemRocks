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
//  assert(status.ok());
  if (!status.ok()) std::cerr << status.ToString() << std::endl;

  std::string value;
  std::string key;
  auto option_db = rocksdb::WriteOptions();
  option_db.disableWAL = true;
  rocksdb::Status s = db->Put(option_db, "StartKey", "StartValue");
  for (int i = 0; i<1000000; i++){
    key = std::to_string(i);
    value = std::to_string(i);
    if (s.ok()) s = db->Put(option_db, key, value);
    std::cout << "iteration number " << i << std::endl;
  }
  s = db->Get(rocksdb::ReadOptions(), "50", &value);
  if(s.ok()) std::cout<< value << std::endl;
  else std::cerr << status.ToString() << std::endl;
  s = db->Get(rocksdb::ReadOptions(), "800", &value);
  if(s.ok()) std::cout<< value << std::endl;
  else std::cerr << status.ToString() << std::endl;
  s = db->Get(rocksdb::ReadOptions(), "4000", &value);
  if(s.ok()) std::cout<< value << std::endl;
  else std::cerr << status.ToString() << std::endl;
  s = db->Get(rocksdb::ReadOptions(), "8000", &value);
  if(s.ok()) std::cout<< value << std::endl;
  else std::cerr << status.ToString() << std::endl;
  s = db->Get(rocksdb::ReadOptions(), "700000", &value);
  if(s.ok()) std::cout<< value << std::endl;
  else std::cerr << status.ToString() << std::endl;
  s = db->Get(rocksdb::ReadOptions(), "-10000", &value);
  if(s.ok()) std::cout<< value << std::endl;
  else std::cerr << status.ToString() << std::endl;
  return 0;
}
