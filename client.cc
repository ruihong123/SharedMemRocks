#include <assert.h>

#include <iostream>
#include <string>
#include <thread>
#include <stdlib.h>
#include "rocksdb/db.h"
int main()
{
  rocksdb::DB* db;
  rocksdb::Options options;
  options.create_if_missing = true;
  options.write_buffer_size = 4*1024*1024;
  rocksdb::Status status =
      rocksdb::DB::Open(options, "/tmp/testdb", &db);
//  assert(status.ok());
  if (!status.ok()) std::cerr << status.ToString() << std::endl;
 auto f = [=](int dislocation){
    std::string value;
    std::string key;
    auto option_wr = rocksdb::WriteOptions();
    option_wr.disableWAL = true;
    rocksdb::Status s = db->Put(option_wr, "StartKey", "StartValue");
    s = db->Delete(option_wr, "NewKey");
    for (int i = 0; i<1000000; i++){
      key = std::to_string(i);
      value = std::to_string(i+dislocation);
      if (s.ok()) s = db->Put(option_wr, key, value);
//      std::cout << "iteration number " << i << std::endl;
    }
   for (int i = 1000*dislocation; i<1000*(dislocation+1); i++){
     key = std::to_string(i);
//     value = std::to_string(i + dislocation);
     if (s.ok()) s = db->Delete(option_wr, key);
//     std::cout << "iteration number " << i << std::endl;
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
  };
  std::thread t5(f, 4);
//  std::thread t1(f, 0);
//  std::thread t2(f, 1);
//  std::thread t3(f, 2);
//  std::thread t4(f, 3);


  // Wait for t1 to finish
//  t1.join();
//  t2.join();
//  t3.join();
//  t4.join();
  t5.join();

  return 0;
}


