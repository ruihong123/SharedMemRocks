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
      rocksdb::DB::Open(options, "/tmp/testdb1", &db);
//  assert(status.ok());
  if (!status.ok()) std::cerr << status.ToString() << std::endl;
  std::string value;
  std::string key;
  auto option_wr = rocksdb::WriteOptions();
  option_wr.disableWAL = true;
  rocksdb::Status s = db->Put(option_wr, "StartKey", "StartValue");
  s = db->Delete(option_wr, "NewKey");
  for (int i = 0; i<1000; i++){
    key = std::to_string(i);
    value = std::to_string(i);

    s = db->Put(option_wr, key, value);
    if (!s.ok()){
      std::cerr << s.ToString() << std::endl;
      return 0;
    }

//      std::cout << "iteration number " << i << std::endl;

  }
  for (int i = 0; i<1000; i++){
    key = std::to_string(i);
//     value = std::to_string(i+dislocation);
    s = db->Delete(option_wr, key);
    if (!s.ok()){
      std::cerr << s.ToString() << std::endl;
      return 0;
    }

//     std::cout << "Delete iteration number " << i << std::endl;
  }
  for (int i = 1001; i<5000000; i++){
    key = std::to_string(i);
    value = std::to_string(i);
    s = db->Put(option_wr, key, value);
    if (!s.ok()){
      std::cerr << s.ToString() << std::endl;
      return 0;
    }

//     std::cout << "iteration number " << i << std::endl;
  }
  for (int i = 1000; i<1000+1000; i++){
    key = std::to_string(i);
//     value = std::to_string(i + dislocation);
    s = db->Delete(option_wr, key);
    if (!s.ok()){
      std::cerr << s.ToString() << std::endl;
      return 0;
    }
//     std::cout << "iteration number " << i << std::endl;
  }
  auto start = std::chrono::high_resolution_clock::now();

  auto f = [&](int dislocation){
    auto myid = std::this_thread::get_id();
    std::stringstream ss;
    ss << myid;
    auto* posix_tid = new std::string(ss.str());
    rocksdb::FileSystem::Default()->rdma_mg->Remote_Query_Pair_Connection(*posix_tid);
    rocksdb::FileSystem::Default()->rdma_mg->t_local_1->Reset(posix_tid);
//    std::string value;
//    std::string key;
//    rocksdb::Status s = db->Put(option_wr, "StartKey", "StartValue");
    for (int i = 0; i<5000000; i++) {
      key = std::to_string(( std::rand() % ( 5000001 ) ));
//      value = std::to_string(i+dislocation);
      s = db->Get(rocksdb::ReadOptions(), key, &value);
      if (!s.ok()) {
        std::cerr << s.ToString() << std::endl;
        return;
      }
    }

  };
  std::thread t5(f, 4);
  std::thread t1(f, 0);
  std::thread t2(f, 1);
  std::thread t3(f, 2);
  std::thread t4(f, 3);
//  std::thread t6(f, 4);
//  std::thread t7(f, 0);
//  std::thread t8(f, 1);
//  std::thread t9(f, 2);
//  std::thread t10(f, 3);


  // Wait for t1 to finish
  t1.join();
  t2.join();
  t3.join();
  t4.join();
  t5.join();
  auto stop = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);

  std::cout << duration.count() << std::endl;
  return 0;
}


