#include <iostream>
#include "rocksdb/rdma.h"
//#include <boost/bucket_serialization/map.hpp>
//namespace ROCKSDB_NAMESPACE {
void client_thread(rocksdb::RDMA_Manager* rdma_manager) {
  auto myid = std::this_thread::get_id();
  std::stringstream ss;
  ss << myid;
  auto* thread_id = new std::string(ss.str());
  rdma_manager->Remote_Memory_Register(100 * 1024 * 1024);
  rdma_manager->Remote_Query_Pair_Connection(*thread_id);
  std::cout << rdma_manager->remote_mem_pool[0];
  ibv_mr mem_pool_table[2];
  mem_pool_table[0] = *(rdma_manager->local_mem_pool[0]);
  mem_pool_table[1] = *(rdma_manager->local_mem_pool[0]);
  mem_pool_table[1].addr =
      (void*)((char*)mem_pool_table[1].addr +
              sizeof("message from computing node"));  // PROBLEM Could be here.

  char* msg = static_cast<char*>(rdma_manager->local_mem_pool[0]->addr);
  strcpy(msg, "message from computing node");
  int msg_size = sizeof("message from computing node");
  //  rdma_manager->RDMA_Write(rdma_manager->remote_mem_pool[0], &mem_pool_table[0],
  //                           msg_size, *thread_id);
  //
  //  rdma_manager->RDMA_Read(rdma_manager->remote_mem_pool[0], &mem_pool_table[1],
  //                          msg_size, *thread_id);
  //  ibv_wc* wc = new ibv_wc();
  //
  //    rdma_manager->poll_completion(wc, 2, thread_id);
  //    if (wc->status != 0){
  //      fprintf(stderr, "Work completion status is %d \n", wc->status);
  //      fprintf(stderr, "Work completion opcode is %d \n", wc->opcode);
  //    }

  std::cout << "write buffer: " << (char*)mem_pool_table[0].addr << std::endl;

  std::cout << "read buffer: " << (char*)mem_pool_table[1].addr << std::endl;
}
void initialization(SST_Metadata* meta1, ibv_mr** mr1, size_t size, int id) {
  for (size_t i = 0; i < size; i++) {
    if (i == 0) {
      meta1[i].fname = std::string("meta") + std::to_string(id);
      meta1[i].file_size = 6553600;

    } else
      meta1[i].last_ptr = &meta1[i - 1];
    if (i != size - 1) meta1[i].next_ptr = &meta1[i + 1];

    mr1[i] = new ibv_mr();
    meta1[i].mr = mr1[i];
    meta1[i].map_pointer = mr1[i];
    mr1[i]->rkey = 430;
    mr1[i]->addr = reinterpret_cast<void*>(0x47906);
    mr1[i]->length = 7;
  }
}
//// bucket_serialization for Memory regions
//void mr_serialization(char*& temp, int& size, ibv_mr* mr) {
//  void* p = mr->context;
//  // TODO: It can not be changed into net stream.
//  //    void* p_net = htonll(p);
//  memcpy(temp, &p, sizeof(void*));
//  temp = temp + sizeof(void*);
//  p = mr->pd;
//  memcpy(temp, &p, sizeof(void*));
//  temp = temp + sizeof(void*);
//  p = mr->addr;
//  memcpy(temp, &p, sizeof(void*));
//  temp = temp + sizeof(void*);
//  uint32_t rkey = mr->rkey;
//  uint32_t rkey_net = htonl(rkey);
//  memcpy(temp, &rkey_net, sizeof(uint32_t));
//  temp = temp + sizeof(uint32_t);
//  uint32_t lkey = mr->lkey;
//  uint32_t lkey_net = htonl(lkey);
//  memcpy(temp, &lkey_net, sizeof(uint32_t));
//  temp = temp + sizeof(uint32_t);
//  uint32_t handle = mr->handle;
//  uint32_t handle_net = htonl(handle);
//  memcpy(temp, &handle_net, sizeof(uint32_t));
//  temp = temp + sizeof(uint32_t);
//  size_t length_mr = mr->length;
//  size_t length_mr_net = htonl(length_mr);
//  memcpy(temp, &length_mr_net, sizeof(size_t));
//  temp = temp + sizeof(size_t);
//}
//void mr_deserialization(char*& temp, int& size, ibv_mr*& mr) {
//  void* context_p = nullptr;
//  // TODO: It can not be changed into net stream.
//
//  memcpy(&context_p, temp, sizeof(void*));
//  //    void* p_net = htonll(context_p);
//  temp = temp + sizeof(void*);
//  void* pd_p = nullptr;
//  memcpy(&pd_p, temp, sizeof(void*));
//  temp = temp + sizeof(void*);
//  void* addr_p = nullptr;
//  memcpy(&addr_p, temp, sizeof(void*));
//  temp = temp + sizeof(void*);
//  uint32_t rkey_net;
//  memcpy(&rkey_net, temp, sizeof(uint32_t));
//  uint32_t rkey = htonl(rkey_net);
//  temp = temp + sizeof(uint32_t);
//
//  uint32_t lkey_net;
//  memcpy(&lkey_net, temp, sizeof(uint32_t));
//  uint32_t lkey = htonl(lkey_net);
//  temp = temp + sizeof(uint32_t);
//
//  uint32_t handle_net;
//  memcpy(&handle_net, temp, sizeof(uint32_t));
//  uint32_t handle = htonl(handle_net);
//  temp = temp + sizeof(uint32_t);
//
//  size_t length_mr_net = 0;
//
//  memcpy(&length_mr_net, temp, sizeof(size_t));
//  size_t length_mr = htonl(length_mr_net);
//  temp = temp + sizeof(size_t);
//  mr = new ibv_mr;
//  mr->context = static_cast<ibv_context*>(context_p);
//  mr->pd = static_cast<ibv_pd*>(pd_p);
//  mr->addr = addr_p;
//  mr->rkey = rkey;
//  mr->lkey = lkey;
//  mr->handle = handle;
//  mr->length = length_mr;
//}
//
//void serialization(char*& buff, int& size, std::string dbname,
//                   std::map<std::string, SST_Metadata*>& file_to_sst_meta,
//                   std::map<void*, In_Use_Array>& Remote_Mem_Bitmap) {
//  char* temp = buff;
//  // deserailize the name
//  size_t namenumber = dbname.size();
//  size_t namenumber_net = htonl(namenumber);
//  memcpy(temp, &namenumber_net, sizeof(size_t));
//  temp = temp + sizeof(size_t);
//  memcpy(temp, dbname.c_str(), namenumber);
//  temp = temp + namenumber;
//  // serialize the filename map
//  {
//    size_t filenumber = file_to_sst_meta.size();
//    size_t filenumber_net = htonl(filenumber);
//    memcpy(temp, &filenumber_net, sizeof(size_t));
//    temp = temp + sizeof(size_t);
//    for (auto iter : file_to_sst_meta) {
//      size_t filename_length = iter.first.size();
//      size_t filename_length_net = htonl(filename_length);
//      memcpy(temp, &filename_length_net, sizeof(size_t));
//      temp = temp + sizeof(size_t);
//      memcpy(temp, iter.first.c_str(), filename_length);
//      temp = temp + filename_length;
//      int file_size = iter.second->file_size;
//      int file_size_net = htonl(file_size);
//      memcpy(temp, &file_size_net, sizeof(int));
//      temp = temp + sizeof(int);
//      // check how long is the list
//      SST_Metadata* meta_p = iter.second;
//      SST_Metadata* temp_meta = meta_p;
//      size_t list_len = 1;
//      while (temp_meta->next_ptr != nullptr) {
//        list_len++;
//        temp_meta = temp_meta->next_ptr;
//      }
//      size_t list_len_net = ntohl(list_len);
//      memcpy(temp, &list_len_net, sizeof(size_t));
//      temp = temp + sizeof(size_t);
//      meta_p = iter.second;
//      while (meta_p != nullptr) {
//        mr_serialization(temp, size, meta_p->mr);
//        size_t length_map = meta_p->map_pointer->length;
//        size_t length_map_net = htonl(length_map);
//        memcpy(temp, &length_map_net, sizeof(size_t));
//        temp = temp + sizeof(size_t);
//        void* p = meta_p->map_pointer->addr;
//        memcpy(temp, &p, sizeof(void*));
//        temp = temp + sizeof(void*);
//        meta_p = meta_p->next_ptr;
//      }
//    }
//  }
//  // Serialization for the bitmap
//  size_t bitmap_number = Remote_Mem_Bitmap.size();
//  size_t bitmap_number_net = htonl(bitmap_number);
//  memcpy(temp, &bitmap_number_net, sizeof(size_t));
//  temp = temp + sizeof(size_t);
//  for (auto iter : Remote_Mem_Bitmap) {
//    void* p = iter.first;
//    memcpy(temp, &p, sizeof(void*));
//    temp = temp + sizeof(void*);
//    size_t element_size = iter.second.get_element_size();
//    size_t element_size_net = htonl(element_size);
//    memcpy(temp, &element_size_net, sizeof(size_t));
//    temp = temp + sizeof(size_t);
//    size_t chunk_size = iter.second.get_chunk_size();
//    size_t chunk_size_net = htonl(chunk_size);
//    memcpy(temp, &chunk_size_net, sizeof(size_t));
//    temp = temp + sizeof(size_t);
//    std::atomic<bool>* in_use = iter.second.get_inuse_table();
//    for (unsigned int i = 0; i < element_size; i++) {
//      bool bit_temp = in_use[i];
//      std::cout << "serial" << in_use[i] << std::endl;
//      memcpy(temp, &bit_temp, sizeof(bool));
//      temp = temp + sizeof(bool);
//    }
//    mr_serialization(temp, size, iter.second.get_mr_ori());
//  }
//  size = temp - buff;
//}
//void deserialization(char*& buff, int& size, std::string& db_name,
//                     std::map<std::string, SST_Metadata*>& file_to_sst_meta,
//                     std::map<void*, In_Use_Array>& Remote_Mem_Bitmap) {
//  char* temp = buff;
//  // deserialize the name.
//  size_t namenumber_net;
//  memcpy(&namenumber_net, temp, sizeof(size_t));
//  size_t namenumber = htonl(namenumber_net);
//  temp = temp + sizeof(size_t);
//
//  char dbname_[namenumber + 1];
//  memcpy(dbname_, temp, namenumber);
//  dbname_[namenumber] = '\0';
//  temp = temp + namenumber;
//  assert(db_name == std::string(dbname_));
//  // deserialize the fs.
//  size_t filenumber_net;
//  memcpy(&filenumber_net, temp, sizeof(size_t));
//  size_t filenumber = htonl(filenumber_net);
//  temp = temp + sizeof(size_t);
//  for (size_t i = 0; i < filenumber; i++) {
//    size_t filename_length_net;
//    memcpy(&filename_length_net, temp, sizeof(size_t));
//    size_t filename_length = ntohl(filename_length_net);
//    temp = temp + sizeof(size_t);
//    char filename[filename_length + 1];
//    memcpy(filename, temp, filename_length);
//    filename[filename_length] = '\0';
//    temp = temp + filename_length;
//
//    int file_size_net = 0;
//    memcpy(&file_size_net, temp, sizeof(int));
//    int file_size = ntohl(file_size_net);
//    temp = temp + sizeof(int);
//    size_t list_len_net = 0;
//    memcpy(&list_len_net, temp, sizeof(size_t));
//    size_t list_len = htonl(list_len_net);
//    temp = temp + sizeof(size_t);
//    SST_Metadata* meta_head;
//    SST_Metadata* meta = new SST_Metadata();
//    meta->file_size = file_size;
//    meta_head = meta;
//    for (size_t j = 0; j < list_len; j++) {
//      // below could be problematic.
//      meta->fname = std::string(filename);
//      mr_deserialization(temp, size, meta->mr);
//      size_t length_map_net = 0;
//      memcpy(&length_map_net, temp, sizeof(size_t));
//      size_t length_map = htonl(length_map_net);
//      temp = temp + sizeof(size_t);
//      void* start_key;
//      memcpy(&start_key, temp, sizeof(void*));
//      temp = temp + sizeof(void*);
//      meta->map_pointer = new ibv_mr;
//      *(meta->map_pointer) = *(meta->mr);
//      meta->map_pointer->length = length_map;
//      meta->map_pointer->addr = start_key;
//      if (j != list_len - 1) {
//        meta->next_ptr = new SST_Metadata();
//        meta = meta->next_ptr;
//      }
//    }
//    file_to_sst_meta.insert({std::string(filename), meta_head});
//  }
//  // desirialize the Bit map
//  size_t bitmap_number_net = 0;
//  memcpy(&bitmap_number_net, temp, sizeof(size_t));
//  size_t bitmap_number = htonl(bitmap_number_net);
//  temp = temp + sizeof(size_t);
//  for (size_t i = 0; i < bitmap_number; i++) {
//    void* p_key;
//    memcpy(&p_key, temp, sizeof(void*));
//    temp = temp + sizeof(void*);
//    size_t element_size_net = 0;
//    memcpy(&element_size_net, temp, sizeof(size_t));
//    size_t element_size = htonl(element_size_net);
//    temp = temp + sizeof(size_t);
//    size_t chunk_size_net = 0;
//    memcpy(&chunk_size_net, temp, sizeof(size_t));
//    size_t chunk_size = htonl(chunk_size_net);
//    temp = temp + sizeof(size_t);
//    auto* in_use = new std::atomic<bool>[element_size];
//    bool bit_temp;
//    for (size_t j = 0; j < element_size; j++) {
//      memcpy(&bit_temp, temp, sizeof(bool));
//      in_use[j] = bit_temp;
//      std::cout << "deserial" << in_use[j] << std::endl;
//      temp = temp + sizeof(bool);
//    }
//    ibv_mr* mr_inuse;
//    mr_deserialization(temp, size, mr_inuse);
//    In_Use_Array in_use_array(element_size, chunk_size, mr_inuse, in_use);
//    Remote_Mem_Bitmap.insert({p_key, in_use_array});
//  }
//}
int main() {
  struct config_t config = {
      NULL,  /* dev_name */
      NULL,  /* server_name */
      19875, /* tcp_port */
      1,
      /* ib_port */        // physical
      1,                   /* gid_idx */
      4 * 10 * 1024 * 1024 /*initial local buffer size*/
  };

  //  ibv_mr inuse_mr;
  //  inuse_mr.length = 559;
  //  In_Use_Array inuse_a(100, 1000, &inuse_mr);
  //  Remote_Bitmap->insert({nullptr, inuse_a});
  size_t table_size = 8 * 1024 * 1024;
  std::string dbname_for_test("database");
  rocksdb::RDMA_Manager* rdma_manager =
      new rocksdb::RDMA_Manager(config, table_size, &dbname_for_test);
  rdma_manager->Mempool_initialize(std::string("read"), 8 * 1024);
  rdma_manager->Mempool_initialize(std::string("write"), 1024 * 1024);

  rdma_manager->Client_Set_Up_Resources();
  auto Remote_Bitmap = new rocksdb::RDMA_extensible_hash<void*, In_Use_Array>(
      rdma_manager, 5120, dbname_for_test, 2);
  auto file_to_sst_meta =
      new rocksdb::RDMA_extensible_hash<std::string, SST_Metadata>(
          rdma_manager, 5120, dbname_for_test, 1);
  ibv_mr* mr = new ibv_mr;
  mr->length = 11;
  In_Use_Array in_use(10, 4096, mr);
  for (int i = 0; i < 5; i++) in_use.allocate_memory_slot();
  Remote_Bitmap->insert(static_cast<void*>(&rdma_manager), &in_use);
  size_t buff_size = 1024 * 1024 * 1014;
  char* test_buff = static_cast<char*>(malloc(buff_size));
  int mr_flags =
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
  //  auto start = std::chrono::high_resolution_clock::now();
  //  ibv_reg_mr(rdma_manager->res->pd, test_buff, buff_size, mr_flags);
  //  auto stop = std::chrono::high_resolution_clock::now();
  //  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start); std::printf("RDMA memory register %ld \n", duration.count());


  SST_Metadata meta1[10] = {};
  ibv_mr* mr1[10];
  initialization(meta1, mr1, 10, 1);
  SST_Metadata meta2[3] = {};
  ibv_mr* mr2[3];
  initialization(meta2, mr2, 3, 2);
  SST_Metadata meta3[50] = {};
  ibv_mr* mr3[50];
  initialization(meta3, mr3, 50, 3);
  SST_Metadata meta4[50] = {};
  ibv_mr* mr4[50];
  initialization(meta4, mr4, 50, 4);
  SST_Metadata meta5[50] = {};
  ibv_mr* mr5[50];
  initialization(meta5, mr5, 50, 5);
  SST_Metadata meta6[50] = {};
  ibv_mr* mr6[50];
  initialization(meta6, mr6, 50, 6);
  file_to_sst_meta->insert(meta1[0].fname, meta1);
  file_to_sst_meta->insert(meta2[0].fname, meta2);
  file_to_sst_meta->insert(meta3[0].fname, meta3);
  file_to_sst_meta->insert(meta4[0].fname, meta4);
  file_to_sst_meta->insert(meta5[0].fname, meta5);
  file_to_sst_meta->insert(meta6[0].fname, meta6);
  size_t size = 0;
  char* buff = static_cast<char*>(malloc(1024 * 1024));

//  file_to_sst_meta->get_bucket(0)->bucket_serialization(buff, size);
  file_to_sst_meta->Sync();
//  auto Remote_Bitmap_reopen = new std::map<void*, In_Use_Array>;
//  std::map<std::string, SST_Metadata*> file_to_sst_meta_reopen;
  auto file_to_sst_meta_reopen = new rocksdb::RDMA_extensible_hash<std::string, SST_Metadata>(
      rdma_manager, 5120, dbname_for_test, 1);
  file_to_sst_meta_reopen->get_bucket(0)->bucket_deserialization(buff, size);
  std::cout << file_to_sst_meta_reopen->at("meta3")->file_size << std::endl;
  std::cout << file_to_sst_meta_reopen->at("meta3")->mr->addr << std::endl;
  std::cout << file_to_sst_meta_reopen->at("meta1") << std::endl;
  size = 0;
  Remote_Bitmap->get_bucket(0)->bucket_serialization(buff, size);
  auto Remote_Bitmap_reopen = new rocksdb::RDMA_extensible_hash<void*, In_Use_Array>(
      rdma_manager, 5120, dbname_for_test, 2);
  std::string dbname_return("database");
  Remote_Bitmap_reopen->get_bucket(0)->bucket_deserialization(buff, size);

  std::cout << Remote_Bitmap_reopen->get_bucket(0)->contents->begin()->second->get_mr_ori()->length
            << std::endl;
  std::cout << Remote_Bitmap_reopen->get_bucket(0)->contents->begin()->second->get_inuse_table()[2]
            << std::endl;

  //  while(1);

  return 0;
}
//}