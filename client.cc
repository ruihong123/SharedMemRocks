#include <assert.h>
#include <include/rocksdb/table.h>
#include <stdlib.h>

#include <iostream>
#include <string>
#include <thread>

#include <chrono>

#include "rocksdb/db.h"
#include "rocksdb/options.h"

int main()
{
  rocksdb::DB* db;
  rocksdb::Options options;
  options.create_if_missing = true;
  options.write_buffer_size = 4*1024*1024;
  options.env->SetBackgroundThreads(1, rocksdb::Env::Priority::HIGH);
  options.env->SetBackgroundThreads(1, rocksdb::Env::Priority::LOW);
  rocksdb::BlockBasedTableOptions table_options;
  table_options.checksum= rocksdb::kCRC32c;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
//  options.paranoid_file_checks=true;
//  options.use_direct_reads = true;
  rocksdb::Status status =
      rocksdb::DB::Open(options, "/tmp/testdb", &db);
//  assert(status.ok());
  if (!status.ok()) std::cerr << status.ToString() << std::endl;
  using namespace std::chrono;
  auto start = high_resolution_clock::now();
  auto f = [=](int dislocation){
    std::string value;
    std::string key;
    auto option_wr = rocksdb::WriteOptions();
//    option_wr.disableWAL = true;
    rocksdb::Status s = db->Put(option_wr, "StartKey", "StartValue");
    s = db->Delete(option_wr, "NewKey");
    for (int i = 0; i<1000; i++){
      key = std::to_string(i);
      value = std::to_string(i+dislocation);

      s = db->Put(option_wr, key, value);
      if (!s.ok()){
        std::cerr << s.ToString() << std::endl;
        return;
      }

//      std::cout << "iteration number " << i << std::endl;

    }
   for (int i = 0; i<1000; i++){
     key = std::to_string(i);
//     value = std::to_string(i+dislocation);
     s = db->Delete(option_wr, key);
     if (!s.ok()){
       std::cerr << s.ToString() << std::endl;
       return;
     }

//     std::cout << "Delete iteration number " << i << std::endl;
   }
   for (int i = 1001; i<5000000; i++){
     key = std::to_string(i);
     value = std::to_string(i+dislocation);
     s = db->Put(option_wr, key, value);
     if (!s.ok()){
       std::cerr << s.ToString() << std::endl;
       return;
     }

//     std::cout << "iteration number " << i << std::endl;
   }
   for (int i = 1000+1000*dislocation; i<1000+1000*(dislocation+1); i++){
     key = std::to_string(i);
//     value = std::to_string(i + dislocation);
     s = db->Delete(option_wr, key);
     if (!s.ok()){
       std::cerr << s.ToString() << std::endl;
       return;
     }
//     std::cout << "iteration number " << i << std::endl;
   }
    s = db->Get(rocksdb::ReadOptions(), "50", &value);
    if(s.ok()) std::cout<< value << std::endl;
    else std::cerr << s.ToString() << std::endl;
    s = db->Get(rocksdb::ReadOptions(), "800", &value);
    if(s.ok()) std::cout<< value << std::endl;
    else std::cerr << s.ToString() << std::endl;
    s = db->Get(rocksdb::ReadOptions(), "1100", &value);
    if(s.ok()) std::cout<< value << std::endl;
    else std::cerr << s.ToString() << std::endl;
    s = db->Get(rocksdb::ReadOptions(), "8000", &value);
    if(s.ok()) std::cout<< value << std::endl;
    else std::cerr << s.ToString() << std::endl;
     s = db->Get(rocksdb::ReadOptions(), "20000", &value);
     if(s.ok()) std::cout<< value << std::endl;
     else std::cerr << s.ToString() << std::endl;
    s = db->Get(rocksdb::ReadOptions(), "100000", &value);
    if(s.ok()) std::cout<< value << std::endl;
    else std::cerr << s.ToString() << std::endl;
    s = db->Get(rocksdb::ReadOptions(), "300000", &value);
    if(s.ok()) std::cout<< value << std::endl;
    else std::cerr << s.ToString() << std::endl;
    s = db->Get(rocksdb::ReadOptions(), "700000", &value);
    if(s.ok()) std::cout<< value << std::endl;
    else std::cerr << s.ToString() << std::endl;
    s = db->Get(rocksdb::ReadOptions(), "-10000", &value);
    if(s.ok()) std::cout<< value << std::endl;
    else std::cerr << s.ToString() << std::endl;
  };
  std::thread t5(f, 5);
  std::thread t1(f, 0);
  std::thread t2(f, 1);
  std::thread t3(f, 2);
  std::thread t4(f, 3);


  // Wait for t1 to finish
  t1.join();
  t2.join();
  t3.join();
  t4.join();
  t5.join();
  auto stop = high_resolution_clock::now();
  auto duration = duration_cast<microseconds>(stop - start);

  std::cout << duration.count() << std::endl;

  return 0;
}


