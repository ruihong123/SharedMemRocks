#ifndef RDMA_H
#define RDMA_H



#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <inttypes.h>
#include <endian.h>
#include <byteswap.h>
//#include <getopt.h>
#include <cassert>
//#include <unordered_map>
#include <algorithm>
#include <shared_mutex>
#include <thread>
#include <chrono>
#include <memory>
#include <sstream>
#include <set>
//#include <options.h>

#include <arpa/inet.h>
#include <infiniband/verbs.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include "util/thread_local.h"
#include <atomic>
#include <chrono>
#include <iostream>
#include <map>
#include <vector>
//#ifdef __cplusplus
//extern "C" { //only need to export C interface if
//// used by C++ source code
//#endif

/* poll CQ timeout in millisec (2 seconds) */
#define MAX_POLL_CQ_TIMEOUT 1000000
#define MSG "SEND operation "


#if __BYTE_ORDER == __LITTLE_ENDIAN
template <typename T>
  static inline T hton(T u) {
  static_assert (CHAR_BIT == 8, "CHAR_BIT != 8");

  union
  {
    T u;
    unsigned char u8[sizeof(T)];
  } source, dest;

  source.u = u;

  for (size_t k = 0; k < sizeof(T); k++)
    dest.u8[k] = source.u8[sizeof(T) - k - 1];

  return dest.u;
}
//  static inline uint64_t htonll(uint64_t x) { return bswap_64(x); }
//  static inline uint64_t ntohll(uint64_t x) { return bswap_64(x); }
#elif __BYTE_ORDER == __BIG_ENDIAN
  static inline uint64_t htonll(uint64_t x) { return x; }
  static inline uint64_t ntohll(uint64_t x) { return x; }
#else
#error __BYTE_ORDER is neither __LITTLE_ENDIAN nor __BIG_ENDIAN
#endif

struct config_t
{
  const char* dev_name; /* IB device name */
  const char* server_name;	/* server host name */
  u_int32_t tcp_port;   /* server TCP port */
  int ib_port;		  /* local IB port to work with, or physically port number */
  int gid_idx;		  /* gid index to use */
  int init_local_buffer_size;   /*initial local SST buffer size*/
};
/* structure to exchange data which is needed to connect the QPs */
struct registered_qp_config {
  uint32_t qp_num; /* QP number */
  uint16_t lid;	/* LID of the IB port */
  uint8_t gid[16]; /* gid */
} __attribute__((packed));
struct change_remote_buckets_mem{
  int hash_id;
  int global_depth;
  size_t bucket_size;
} __attribute__((packed));
struct directory_change{
    int hash_id;
    size_t directory_size;
} __attribute__((packed));
enum RDMA_Command_Type {create_qp_, create_mr_, save_serialized_data,
                         retrieve_serialized_data, remote_hash_region_change, hash_directory_update,
                        retrieve_remote_hash};

union RDMA_Command_Content{
  size_t mem_size;
  registered_qp_config qp_config;
  int data_size;
  change_remote_buckets_mem bucket_change_command;
  directory_change dir_change;
};
struct computing_to_memory_msg
{
  RDMA_Command_Type command;
  RDMA_Command_Content content;
};

// Structure for the file handle in RDMA file system. it could be a link list
// for large files
class SST_Metadata{
 public:
  std::shared_mutex file_lock;
  std::string fname;
  ibv_mr* mr;
  ibv_mr* map_pointer;
  SST_Metadata* last_ptr = nullptr;
  SST_Metadata* next_ptr = nullptr;
  unsigned int file_size = 0;
  void serialization(char*& temp, size_t& size){
    char* start = temp;
    size_t filename_length = fname.size();
    size_t filename_length_net = htonl(filename_length);
    memcpy(temp, &filename_length_net, sizeof(size_t));
    temp = temp + sizeof(size_t);

    memcpy(temp, fname.c_str(), filename_length);
    temp = temp + filename_length;

    unsigned int filesize = this->file_size;
    unsigned int file_size_net = htonl(filesize);
    memcpy(temp, &file_size_net, sizeof(unsigned int));
    temp = temp + sizeof(unsigned int);

    // check how long is the list
    SST_Metadata* meta_p = this;
    SST_Metadata* temp_meta = meta_p;
    size_t list_len = 1;
    while (temp_meta->next_ptr != nullptr) {
      list_len++;
      temp_meta = temp_meta->next_ptr;
    }
    size_t list_len_net = ntohl(list_len);
    memcpy(temp, &list_len_net, sizeof(size_t));
    temp = temp + sizeof(size_t);

    meta_p = this;
    size_t length_map = meta_p->map_pointer->length;
    size_t length_map_net = htonl(length_map);
    memcpy(temp, &length_map_net, sizeof(size_t));
    temp = temp + sizeof(size_t);

    //Here we put context pd handle and length outside the bucket_serialization because we do not need
    void* p = meta_p->mr->context;
    //TODO: It can not be changed into net stream.
//    void* p_net = htonll(p);
    memcpy(temp, &p, sizeof(void*));
    temp = temp + sizeof(void*);

    p = meta_p->mr->pd;
    memcpy(temp, &p, sizeof(void*));
    temp = temp + sizeof(void*);

    uint32_t handle = meta_p->mr->handle;
    uint32_t handle_net = htonl(handle);
    memcpy(temp, &handle_net, sizeof(uint32_t));
    temp = temp + sizeof(uint32_t);

    size_t length_mr = meta_p->mr->length;
    size_t length_mr_net = htonl(length_mr);
    memcpy(temp, &length_mr_net, sizeof(size_t));
    temp = temp + sizeof(size_t);

    while (meta_p != nullptr) {
      p = meta_p->mr->addr;
      memcpy(temp, &p, sizeof(void*));
      temp = temp + sizeof(void*);
      uint32_t rkey = meta_p->mr->rkey;
      uint32_t rkey_net = htonl(rkey);
      memcpy(temp, &rkey_net, sizeof(uint32_t));
      temp = temp + sizeof(uint32_t);
      uint32_t lkey = meta_p->mr->lkey;
      uint32_t lkey_net = htonl(lkey);
      memcpy(temp, &lkey_net, sizeof(uint32_t));
      temp = temp + sizeof(uint32_t);
      p = meta_p->map_pointer->addr;
      memcpy(temp, &p, sizeof(void*));
      temp = temp + sizeof(void*);
      meta_p = meta_p->next_ptr;


    }
    size = temp - start;
    printf("SStmeta size is %zu:", size);
  }
  void deserialization(char*& temp, size_t& size, std::string& name) {
    size_t filename_length_net;
    memcpy(&filename_length_net, temp, sizeof(size_t));
    size_t filename_length = ntohl(filename_length_net);
    temp = temp + sizeof(size_t);

    char filename[filename_length+1];
    memcpy(filename, temp, filename_length);
    filename[filename_length] = '\0';
    temp = temp + filename_length;

    name = std::string(filename);

    unsigned int file_size_net = 0;
    memcpy(&file_size_net, temp, sizeof(unsigned int));
    unsigned int filesize = ntohl(file_size_net);
    temp = temp + sizeof(unsigned int);

    size_t list_len_net = 0;
    memcpy(&list_len_net, temp, sizeof(size_t));
    size_t list_len = htonl(list_len_net);
    temp = temp + sizeof(size_t);

//    SST_Metadata* meta_head;
    SST_Metadata* meta = this;

    meta->file_size = filesize;

//    meta_head = meta;
    size_t length_map_net = 0;
    memcpy(&length_map_net, temp, sizeof(size_t));
    size_t length_map = htonl(length_map_net);
    temp = temp + sizeof(size_t);

    void* context_p = nullptr;
    //TODO: It can not be changed into net stream.
    memcpy(&context_p, temp, sizeof(void*));
//    void* p_net = htonll(context_p);
    temp = temp + sizeof(void*);

    void* pd_p = nullptr;
    memcpy(&pd_p, temp, sizeof(void*));
    temp = temp + sizeof(void*);

    uint32_t handle_net;
    memcpy(&handle_net, temp,  sizeof(uint32_t));
    uint32_t handle = htonl(handle_net);
    temp = temp + sizeof(uint32_t);

    size_t length_mr_net = 0;
    memcpy(&length_mr_net, temp, sizeof(size_t));
    size_t length_mr = htonl(length_mr_net);
    temp = temp + sizeof(size_t);

    for (size_t j = 0; j<list_len; j++){
      meta->mr = new ibv_mr;
      meta->mr->context = static_cast<ibv_context*>(context_p);
      meta->mr->pd = static_cast<ibv_pd*>(pd_p);
      meta->mr->handle = handle;
      meta->mr->length = length_mr;
      //below could be problematic.
      meta->fname = std::string(filename);
      void* addr_p = nullptr;
      memcpy(&addr_p, temp, sizeof(void*));
      temp = temp + sizeof(void*);

      uint32_t rkey_net;
      memcpy(&rkey_net, temp, sizeof(uint32_t));
      uint32_t rkey = htonl(rkey_net);
      temp = temp + sizeof(uint32_t);

      uint32_t lkey_net;
      memcpy(&lkey_net, temp, sizeof(uint32_t));
      uint32_t lkey = htonl(lkey_net);
      temp = temp + sizeof(uint32_t);

      meta->mr->addr = addr_p;
      meta->mr->rkey = rkey;
      meta->mr->lkey = lkey;
      meta->map_pointer = new ibv_mr;
      *(meta->map_pointer) = *(meta->mr);

      void* start_key;
      memcpy(&start_key, temp, sizeof(void*));
      temp = temp + sizeof(void*);

      meta->map_pointer->length = length_map;
      meta->map_pointer->addr = start_key;
      if (j!=list_len-1){
        meta->next_ptr = new SST_Metadata();
        meta = meta->next_ptr;
      }

    }
//    file_name_map.insert({std::string(filename), meta_head});
  }

};


/* structure of system resources */
struct resources
{
  union ibv_gid my_gid;
  struct ibv_device_attr device_attr;
  /* Device attributes */
  struct ibv_sge* sge = nullptr;
  struct ibv_recv_wr*	rr = nullptr;
  struct ibv_port_attr port_attr;	/* IB port attributes */
//  std::vector<registered_qp_config> remote_mem_regions; /* memory buffers for RDMA */
  struct ibv_context* ib_ctx = nullptr;		   /* device handle */
  struct ibv_pd* pd = nullptr;				   /* PD handle */
  std::map<std::string,ibv_cq*> cq_map;				   /* CQ Map */
  std::map<std::string,ibv_qp*> qp_map;				   /* QP Map */
  struct ibv_mr* mr_receive = nullptr;              /* MR handle for receive_buf */
  struct ibv_mr* mr_send = nullptr;                 /* MR handle for send_buf */
//  struct ibv_mr* mr_SST = nullptr;                        /* MR handle for SST_buf */
//  struct ibv_mr* mr_remote;                     /* remote MR handle for computing node */
  char* SST_buf = nullptr;			/* SSTable buffer pools pointer, it could contain multiple SSTbuffers */
  char* send_buf = nullptr;                       /* SEND buffer pools pointer, it could contain multiple SEND buffers */
  char* receive_buf = nullptr;		        /* receive buffer pool pointer,  it could contain multiple acturall receive buffers */
  std::map<std::string, int> sock_map;						   /* TCP socket file descriptor */
  std::map<std::string, ibv_mr*> mr_receive_map;
  std::map<std::string, ibv_mr*> mr_send_map;

};
class In_Use_Array{
 public:
//  RDMA_extensible_hash<void*, In_Use_Array*>* extensible_hash;
  In_Use_Array() {}
  In_Use_Array(size_t size, size_t chunk_size, ibv_mr* mr_ori)
      :element_size_(size), chunk_size_(chunk_size), mr_ori_(mr_ori){
    in_use_ = new std::atomic<bool>[element_size_];
    for (size_t i = 0; i < element_size_; ++i){
      in_use_[i] = false;
    }

  }
  In_Use_Array(size_t size, size_t chunk_size, ibv_mr* mr_ori,
               std::atomic<bool>* in_use)
      :element_size_(size), chunk_size_(chunk_size), in_use_(in_use), mr_ori_(mr_ori){

  }
  int allocate_memory_slot(){
    for (int i = 0; i < static_cast<int>(element_size_); ++i){
//      auto start = std::chrono::high_resolution_clock::now();
      bool temp = in_use_[i];
      if (temp == false) {
//        auto stop = std::chrono::high_resolution_clock::now();
//        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//        std::printf("Compare and swap time duration is %ld \n", duration.count());
        if(in_use_[i].compare_exchange_strong(temp, true)){
//          std::cout << "chunk" <<i << "was changed to true" << std::endl;

          return i; // find the empty slot then return the index for the slot

        }
//        else
//          std::cout << "Compare and swap fail" << "i equals" << i  << "type is" << type_ << std::endl;
      }

    }
    return -1; //Not find the empty memory chunk.
  }
  bool deallocate_memory_slot(int index) {
    bool temp = true;
    assert(in_use_[index] == true);
//    std::cout << "chunk" <<index << "was changed to false" << std::endl;

    return in_use_[index].compare_exchange_strong(temp, false);

  }
  size_t get_chunk_size(){
    return chunk_size_;
  }
  ibv_mr* get_mr_ori(){
    return mr_ori_;
  }
  size_t get_element_size(){
    return element_size_;
  }
  std::atomic<bool>* get_inuse_table(){
    return in_use_;
  }
  void serialization(char*& temp, size_t& size) {
    char* start = temp;
    void* p = mr_ori_->addr;
    memcpy(temp, &p, sizeof(void*));
    temp = temp + sizeof(void*);
    size_t element_size = this->get_element_size();
    size_t element_size_net = htonl(element_size);
    memcpy(temp, &element_size_net, sizeof(size_t));
    temp = temp + sizeof(size_t);
    size_t chunk_size = this->get_chunk_size();
    size_t chunk_size_net = htonl(chunk_size);
    memcpy(temp, &chunk_size_net, sizeof(size_t));
    temp = temp + sizeof(size_t);
    std::atomic<bool>* in_use = this->get_inuse_table();
    auto mr = this->get_mr_ori();
    p = mr->context;
    //TODO: It can not be changed into net stream.
//    void* p_net = htonll(p);
    memcpy(temp, &p, sizeof(void*));
    temp = temp + sizeof(void*);

    p = mr->pd;
    memcpy(temp, &p, sizeof(void*));
    temp = temp + sizeof(void*);

    uint32_t handle = mr->handle;
    uint32_t handle_net = htonl(handle);
    memcpy(temp, &handle_net, sizeof(uint32_t));
    temp = temp + sizeof(uint32_t);

    size_t length_mr = mr->length;
    size_t length_mr_net = htonl(length_mr);
    memcpy(temp, &length_mr_net, sizeof(size_t));
    temp = temp + sizeof(size_t);

    p = mr->addr;
    memcpy(temp, &p, sizeof(void*));
    temp = temp + sizeof(void*);

    uint32_t rkey = mr->rkey;
    uint32_t rkey_net = htonl(rkey);
    memcpy(temp, &rkey_net, sizeof(uint32_t));
    temp = temp + sizeof(uint32_t);

    uint32_t lkey = mr->lkey;
    uint32_t lkey_net = htonl(lkey);
    memcpy(temp, &lkey_net, sizeof(uint32_t));
    temp = temp + sizeof(uint32_t);

    for (size_t i = 0; i<element_size; i++){

      bool bit_temp = in_use[i];
      memcpy(temp, &bit_temp, sizeof(bool));
      temp = temp + sizeof(bool);
    }
    size = temp - start;
    printf("In use bitmap size is %zu:", size);
  }

  void deserialization(char*& temp, size_t& size, void*& pointer) {

//    void* p_key;

    memcpy(&pointer, temp, sizeof(void*));
    temp = temp + sizeof(void*);

    size_t element_size_net = 0;
    memcpy(&element_size_net, temp, sizeof(size_t));
    size_t element_size = htonl(element_size_net);
    temp = temp + sizeof(size_t);

    size_t chunk_size_net = 0;
    memcpy(&chunk_size_net, temp, sizeof(size_t));
    size_t chunk_size = htonl(chunk_size_net);
    temp = temp + sizeof(size_t);

    auto* in_use = new std::atomic<bool>[element_size];

    void* context_p = nullptr;
    //TODO: It can not be changed into net stream.
    memcpy(&context_p, temp, sizeof(void*));
//    void* p_net = htonll(context_p);
    temp = temp + sizeof(void*);

    void* pd_p = nullptr;
    memcpy(&pd_p, temp, sizeof(void*));
    temp = temp + sizeof(void*);

    uint32_t handle_net;
    memcpy(&handle_net, temp,  sizeof(uint32_t));
    uint32_t handle = htonl(handle_net);
    temp = temp + sizeof(uint32_t);

    size_t length_mr_net = 0;
    memcpy(&length_mr_net, temp, sizeof(size_t));
    size_t length_mr = htonl(length_mr_net);
    temp = temp + sizeof(size_t);
    auto* mr_inuse = new ibv_mr{0};
    mr_inuse->context = static_cast<ibv_context*>(context_p);
    mr_inuse->pd = static_cast<ibv_pd*>(pd_p);
    mr_inuse->handle = handle;
    mr_inuse->length = length_mr;
    bool bit_temp;
    void* addr_p = nullptr;
    memcpy(&addr_p, temp, sizeof(void*));
    temp = temp + sizeof(void*);

    uint32_t rkey_net;
    memcpy(&rkey_net, temp, sizeof(uint32_t));
    uint32_t rkey = htonl(rkey_net);
    temp = temp + sizeof(uint32_t);

    uint32_t lkey_net;
    memcpy(&lkey_net, temp, sizeof(uint32_t));
    uint32_t lkey = htonl(lkey_net);
    temp = temp + sizeof(uint32_t);

    mr_inuse->addr = addr_p;
    mr_inuse->rkey = rkey;
    mr_inuse->lkey = lkey;
    for (size_t j = 0; j < element_size; j++){
      memcpy(&bit_temp, temp, sizeof(bool));
      in_use[j] = bit_temp;
      temp = temp + sizeof(bool);
    }
    this->element_size_ = element_size;
    this->chunk_size_ = chunk_size;
    this->in_use_ = in_use;
    this->mr_ori_ = mr_inuse;
    // better to use move syntax.
//    in_use_map.insert({p_key, *this});
  }
 private:
  size_t element_size_;
  size_t chunk_size_;
  std::atomic<bool>* in_use_;
  ibv_mr* mr_ori_;
//  int type_;
};
namespace ROCKSDB_NAMESPACE {
/* structure of test parameters */
class RDMA_Manager{
 public:
  RDMA_Manager(config_t config, size_t table_size, std::string* db_name);
  //  RDMA_Manager(config_t config) : rdma_config(config){
//    res = new resources();
//    res->sock = -1;
//  }
//  RDMA_Manager()=delete;
  ~RDMA_Manager();
  // RDMA set up create all the resources, and create one query pair for RDMA send & Receive.
  void Client_Set_Up_Resources();
  //Set up the socket connection to remote shared memory.
  bool Client_Connect_to_Server_RDMA();
  // client function to retrieve serialized data.
  bool client_retrieve_serialized_data(const std::string& db_name, char*& buff,
                                       size_t& buff_size, ibv_mr*& local_mr);
  // client function to save serialized data.
  bool client_save_serialized_data(const std::string& db_name,
                                   char* buff,
                                   size_t buff_size);
  // this function is for the server.
  void Server_to_Client_Communication();
  void server_communication_thread(std::string client_ip, int socket_fd);
  // Local memory register will register RDMA memory in local machine,
  //Both Computing node and share memory will call this function.
  // it also push the new block bit map to the Remote_Mem_Bitmap

  // Set the type of the memory pool. the mempool can be access by the pool name
  bool Mempool_initialize(std::string pool_name, size_t size);
  bool Local_Memory_Register(
      char** p2buffpointer, ibv_mr** p2mrpointer, size_t size,
      std::string pool_name);// register the memory on the local side
  // Remote Memory registering will call RDMA send and receive to the remote memory
  // it also push the new SST bit map to the Remote_Mem_Bitmap
  bool Remote_Memory_Register(size_t size);
  //used for remote hash.
  bool Remote_Memory_Register(size_t size, ibv_mr*& remote_mr);
  int Remote_Memory_Deregister();
  // new query pair creation and connection to remote Memory by RDMA send and receive
  bool Remote_Query_Pair_Connection(
      std::string& qp_id);// Only called by client.

  int RDMA_Read(ibv_mr *remote_mr, ibv_mr *local_mr, size_t msg_size, std::string q_id, size_t send_flag,
                int poll_num);
  int RDMA_Write(ibv_mr* remote_mr, ibv_mr* local_mr, size_t msg_size,
                 std::string q_id, size_t send_flag,
                 int poll_num);
  int RDMA_Send();
  int poll_completion(ibv_wc* wc_p, int num_entries, std::string q_id);
  bool Deallocate_Local_RDMA_Slot(ibv_mr* mr, ibv_mr* map_pointer,
                                  std::string buffer_type);
  bool Deallocate_Local_RDMA_Slot(void* p, std::string buff_type);
  bool Deallocate_Remote_RDMA_Slot(SST_Metadata* sst_meta);
  void fs_meta_save(){
    std::shared_lock<std::shared_mutex> read_lock(*fs_mutex_);
    //TODO: make the buff size dynamically changed, otherwise there will be bug of buffer overflow.
    char* buff = static_cast<char*>(malloc(1024*1024));
    size_t size_dummy;
    fs_serialization(buff, size_dummy, *db_name_, *file_to_sst_meta_, *(Remote_Mem_Bitmap));
    printf("Serialized data size: %zu", size_dummy);
    client_save_serialized_data(*db_name_, buff, size_dummy);}
  //Allocate an empty remote SST, return the index for the memory slot
  void Allocate_Remote_RDMA_Slot(const std::string &file_name,
                                 SST_Metadata*& sst_meta);
  void Allocate_Local_RDMA_Slot(ibv_mr*& mr_input, ibv_mr*& map_pointer,
                                std::string pool_name);
  // this function will determine whether the pointer is with in the registered memory
  bool CheckInsideLocalBuff(void* p, std::_Rb_tree_iterator<std::pair<void * const, In_Use_Array>>& mr_iter,
                            std::map<void*, In_Use_Array>* Bitmap);
  void mr_serialization(char*& temp, size_t& size, ibv_mr* mr);
  void mr_deserialization(char*& temp, size_t& size, ibv_mr*& mr);

  void fs_serialization(char*& buff, size_t& size, std::string& db_name,
      std::unordered_map<std::string, SST_Metadata*>& file_to_sst_meta, std::map<void*, In_Use_Array>& remote_mem_bitmap);
  //Deserialization for linked file is problematic because different file may link to the same SSTdata
  void fs_deserilization(char*& buff, size_t& size, std::string& db_name,
      std::unordered_map<std::string, SST_Metadata*>& file_to_sst_meta,
                         std::map<void*, In_Use_Array>& remote_mem_bitmap,
                         ibv_mr* local_mr);
//  void remote_bucket_change(int hash_id, int global_depth, size_t bucket_size,
//                            ibv_mr*& remote_bucket_mr, std::string& db_name);
  template <typename T>
  int post_send(ibv_mr* mr, std::string qp_id = "main");
  int post_send(ibv_mr* mr, std::string qp_id = "main", size_t size = 0);
//  int post_receives(int len);
  template <typename T>
  int post_receive(ibv_mr* mr, std::string qp_id = "main");
  int post_receive(ibv_mr* mr, std::string qp_id = "main", size_t size = 0);
  int post_receive(ibv_mr** mr_list, size_t sge_size, std::string qp_id);
  int post_send(ibv_mr** mr_list, size_t sge_size, std::string qp_id);
//  void mem_pool_serialization
    //TODO: Make all the variable more smart pointers.
  resources* res = nullptr;
  std::vector<ibv_mr*> remote_mem_pool; /* a vector for all the remote memory regions*/
  std::vector<ibv_mr*> local_mem_pool; /* a vector for all the local memory regions.*/
  std::map<void*, In_Use_Array>* Remote_Mem_Bitmap = nullptr;

//  std::shared_mutex remote_pool_mutex;
//  std::map<void*, In_Use_Array>* Write_Local_Mem_Bitmap = nullptr;
////  std::shared_mutex write_pool_mutex;
//  std::map<void*, In_Use_Array>* Read_Local_Mem_Bitmap = nullptr;
//  std::shared_mutex read_pool_mutex;
//  size_t Read_Block_Size;
//  size_t Write_Block_Size;
  uint64_t Table_Size;
  std::shared_mutex remote_mem_mutex;

  std::shared_mutex rw_mutex;
  std::shared_mutex main_qp_mutex;
  std::shared_mutex qp_cq_map_mutex;
  std::vector<std::thread> thread_pool;
  ThreadLocalPtr* t_local_1;
  ThreadLocalPtr* qp_local;
  ThreadLocalPtr* cq_local;
  std::unordered_map<std::string, std::map<void*, In_Use_Array>> name_to_mem_pool;
  std::unordered_map<std::string, size_t> name_to_size;
  std::shared_mutex local_mem_mutex;
  std::unordered_map<std::string, ibv_mr*> fs_image;
  std::unordered_map<std::string, ibv_mr*> hash_directory_image;
  std::unordered_map<std::string, ibv_mr*> hash_buckets_image;
  std::shared_mutex fs_image_mutex;
  // use thread local qp and cq instead of map, this could be lock free.
//  static __thread std::string thread_id;
 private:

  config_t rdma_config;
  // three variables below are from rdma file system.
  std::string* db_name_;
  std::unordered_map<std::string, SST_Metadata*>* file_to_sst_meta_;
  std::shared_mutex* fs_mutex_;
  int client_sock_connect(const char* servername, int port);
  int server_sock_connect(const char* servername, int port);
  int sock_sync_data(int sock, int xfer_size, char* local_data, char* remote_data);


  int resources_create();
  int modify_qp_to_init(struct ibv_qp* qp);
  int modify_qp_to_rtr(struct ibv_qp* qp, uint32_t remote_qpn, uint16_t dlid, uint8_t* dgid);
  int modify_qp_to_rts(struct ibv_qp* qp);
  bool create_qp(std::string& id);
  int connect_qp(registered_qp_config remote_con_data, std::string& qp_id);
  int resources_destroy();
  void print_config(void);
  void usage(const char* argv0);
};
// type V has not to be pointer, but in fact the content
template <typename K,typename V>
class RDMA_extensible_hash{
 class Bucket;
 public:
  // constructor intitialize the bucket top number as zero
  RDMA_extensible_hash(RDMA_Manager* rdma_mg, size_t bucket_size, std::string dbname, int type):
             rdma_mg_(rdma_mg), globalDepth(0), bucketSize(bucket_size),
        buckettop((1<<globalDepth) - 1),
              db_name(dbname),
        hash_id(type){



    // we set the buffer size as 1.2 time, because I did not want it to have buffer overflow when
    // doing the bucket_serialization.
    void* buff1 = malloc(12*bucketSize);
    void* buff2 = malloc(2*bucketSize);
    memset(buff1, 0, bucketSize);
    memset(buff2, 0, bucketSize);

    int mr_flags =
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
//  auto start = std::chrono::high_resolution_clock::now();
    local_bucket_serialization_mr = ibv_reg_mr(rdma_mg_->res->pd, buff1, 12*bucketSize, mr_flags);
    local_directory_serialization_mr = ibv_reg_mr(rdma_mg_->res->pd, buff2, 2*bucketSize, mr_flags);
    if(!Recover()){
      realloc_remote_bucket_mr();
      ibv_mr* bucket_mem = new ibv_mr();
      *bucket_mem = *remote_bucket_region;
      bucket_mem->length = bucketSize;
      std::shared_ptr<Bucket> temp_bucket = std::make_shared<Bucket>(0,0,bucketSize);
      bucketDirectory.push_back(temp_bucket);
    }
  }
  // helper function to generate hash addressing
  size_t HashKey(const K &key);
  // helper function to get global & local depth
  int GetGlobalDepth() const;
  int GetLocalDepth(int bucket_id) const;
  int GetNumBuckets() const;
  // lookup and modifier
  bool find(const K &key, V* &value);
  bool remove(const K &key);
  void insert(K key, V* value);
  void Sync();
  std::shared_ptr<Bucket>  get_bucket(int i){
    return bucketDirectory[i];
  }
  int BucketIndex(const K &key);
  void bucket_split_or_sync(std::shared_ptr<Bucket> bucket);
  bool Recover();
  void bucket_recover_content(std::shared_ptr<Bucket> bucket);

  V* at(K key);
 private:
  class Bucket{
   public:
    int localDepth;
    std::map<K, V*>* contents;
//    ibv_mr** remote_bucket_region_p;
    size_t bucket_num;
    size_t bucketSize;
    Bucket(int depth, size_t bucketnum, size_t bucketsize)
        : localDepth(depth), bucket_num(bucketnum), bucketSize(bucketsize){
      contents = new std::map<K, V*>();
    }
    ~Bucket(){delete contents;}
    void bucket_serialization(char* buff, size_t& size);
    void bucket_deserialization(char* buff, size_t size);
  };
//  class RemoteBucketDirectory {
//    int global_depths;
//    ibv_mr* mr;
//    RemoteBucketDirectory(int gd, ibv_mr* mem_region):
//        global_depths(gd), mr(mem_region){}
//    void bucket_serialization(char*& buff, size_t& size){
//
//    }
//  };
  RDMA_Manager* rdma_mg_;
  int globalDepth;
  const size_t bucketSize;
  //The number of current bucket number, will be update when bucket split.
  int buckettop;
  std::string db_name;
  int hash_id;
  bool directory_change= false;
//  std::shared_ptr<RemoteBucketDirectory*> remote_bucketdirectory;
  std::vector<std::shared_ptr<Bucket>> bucketDirectory;
  std::set<std::shared_ptr<Bucket>> dirty_buckets;
  // mr for the remote bucket list, we allocate a large chunk at once then allocate it
  //when the buffer overflow, we register a new one (2X bigger) and then copy the old
  //buckets onto it.
  ibv_mr* remote_bucket_region = nullptr;

  // the mr in the local side, which is for serailization. the size is just bucketSize
  ibv_mr* local_bucket_serialization_mr;
  ibv_mr* local_directory_serialization_mr;
  // REmote mr for the hash directory, which
  ibv_mr* remote_directory;
  std::mutex mtx;
  void realloc_remote_bucket_mr();
  void directory_serialization(size_t& data_size);
  // desearialize the directory and recover the local copy of the bucket
  void directory_deserialization();
  void directory_sync();
};

/*
 * helper function to calculate the hashing address of input key
 */
template <typename K, typename V>
size_t RDMA_extensible_hash<K, V>::HashKey(const K &key) {
  return std::hash<K>()(key);
}

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int RDMA_extensible_hash<K, V>::GetGlobalDepth() const {
  return this->globalDepth;
}

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int RDMA_extensible_hash<K, V>::GetLocalDepth(int bucket_id) const {
  return bucketDirectory[bucket_id]->localDepth;
}

/*
 * helper function to return current number of bucket in hash table
 */
template <typename K, typename V>
int RDMA_extensible_hash<K, V>::GetNumBuckets() const {
  return this->buckettop;
}

/*
 * lookup function to find value associate with input key
 */
template <typename K, typename V>
bool RDMA_extensible_hash<K, V>::find(const K &key, V* &value){
  std::lock_guard<std::mutex> lock(this->mtx);
  auto bucket = bucketDirectory[BucketIndex(key)];

  if(bucket == nullptr || bucket->contents->find(key) == bucket->contents->end())
    return false;

  value = (*(bucket->contents))[key];
  return true;
}

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
template <typename K, typename V>
bool RDMA_extensible_hash<K, V>::remove(const K &key) {
  std::lock_guard<std::mutex> lock(this->mtx);
  size_t index = BucketIndex(key);
  auto bucket = bucketDirectory[index];

  if(bucket == nullptr || bucket->contents->find(key) == bucket->contents->end())
    return false;
  bucket->contents->erase(key);
  dirty_buckets.insert(&bucket);
  return true;

}

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
template <typename K, typename V>
void RDMA_extensible_hash<K, V>::insert(K key, V* value) {

  std::lock_guard<std::mutex> lock(this->mtx);
  size_t index = BucketIndex(key);
  std::shared_ptr<Bucket> bucket = bucketDirectory[index];
  index = BucketIndex(key);
  bucket = bucketDirectory[index];
  (*(bucket->contents))[key] = value;
  dirty_buckets.insert(bucket);


}

template  <typename K, typename V>
int RDMA_extensible_hash<K, V>::BucketIndex(const K &key){
  return static_cast<int>(HashKey(key) & ((1 << globalDepth) - 1));
}
template <typename K, typename V>
void RDMA_extensible_hash<K, V>::Sync() {
  for(auto bucket : dirty_buckets) {
      bucket_split_or_sync(bucket);
  }
  if(directory_change){
    directory_sync();
  }
}
template <typename K, typename V>
void RDMA_extensible_hash<K, V>::bucket_split_or_sync(
    std::shared_ptr<Bucket> bucket){
  size_t data_size;
  char* temp = static_cast<char*>(local_bucket_serialization_mr->addr);
  bucket->bucket_serialization(temp, data_size);
  if(data_size > bucketSize){
    directory_change = true;
    if(bucket->localDepth == this->globalDepth){
      // double the bucket size, do the same mapping
      size_t length = bucketDirectory.size();
      for( size_t i = 0; i < length; i++){
        bucketDirectory.push_back(bucketDirectory[i]);
      }
      globalDepth ++;
      realloc_remote_bucket_mr();
      //TODO:this is problematic because the buckets need to modify their remote pointer, to
      // new allocated places.
    }
    buckettop++;
    ibv_mr* new_mr = new ibv_mr();
    *new_mr = *remote_bucket_region;
    new_mr->addr = static_cast<void*>(static_cast<char*>(new_mr->addr) +
                                      buckettop *bucketSize);

    int mask = 1 << bucket->localDepth;
    std::shared_ptr<Bucket> a = std::make_shared<Bucket>(bucket->localDepth + 1, bucket->bucket_num, bucketSize);
    std::shared_ptr<Bucket> b = std::make_shared<Bucket>(bucket->localDepth + 1, buckettop, bucketSize);

    // replace all items in the previous bucket into this new bucket
    for( auto item : *(bucket->contents)){
      size_t newKey = HashKey(item.first);
      if(newKey & mask){
        b->contents->insert(item);
      }else{
        a->contents->insert(item);
      }
    }
    //create 2 bucket to replace last one

    size_t length = bucketDirectory.size();
    for(size_t i = 0; i < length; i++){
      // replace it with new one
      if(bucketDirectory[i] == bucket){
        if(i & mask){
          bucketDirectory[i] = b;
          assert(buckettop = b->bucket_num);
        }else{
          bucketDirectory[i] = a;
        }
      }
    }
    bucket_split_or_sync(a);
    bucket_split_or_sync(b);
  }else{
    ibv_mr remote_bucket;
    remote_bucket = *remote_bucket_region;
    remote_bucket.addr = static_cast<void*>(static_cast<char*>(remote_bucket.addr) + (bucket->bucket_num)*bucketSize);
    rdma_mg_->RDMA_Write(&remote_bucket,
                         local_bucket_serialization_mr, bucketSize,"", IBV_SEND_SIGNALED,1);
  }
}
template <typename K, typename V>
void RDMA_extensible_hash<K, V>::directory_serialization(size_t& data_size) {
  char* buff = static_cast<char*>(local_directory_serialization_mr->addr);
  char* temp = buff;

  size_t namenumber = db_name.size();
  size_t namenumber_net = htonl(namenumber);
  memcpy(temp, &namenumber_net, sizeof(size_t));
  temp = temp + sizeof(size_t);

  memcpy(temp, db_name.c_str(), namenumber);
  temp = temp + namenumber;

  int type_net = htonl(hash_id);
  memcpy(temp, &type_net, sizeof(int));
  temp = temp + sizeof(int);

  std::set<std::shared_ptr<Bucket>> bucket_set(bucketDirectory.begin(), bucketDirectory.end());
  size_t bucketnumber = bucket_set.size();
  size_t bucketnumber_net = htonl(bucketnumber);
  memcpy(temp, &bucketnumber, sizeof(size_t));
  temp = temp + sizeof(size_t);

  for(auto itr: bucket_set){
    void* p = itr.get();
    //TODO: It can not be changed into net stream.
//    void* p_net = htonll(p);
    memcpy(temp, &p, sizeof(void*));
    temp = temp + sizeof(void*);
    memcpy(temp, itr.get(), sizeof(Bucket));
    temp = temp + sizeof(Bucket);
  }
  int globalDepth_net = htonl(globalDepth);
  memcpy(temp, &globalDepth_net, sizeof(int));
  temp = temp + sizeof(int);
  for(auto itr: bucketDirectory){
    void* p = itr.get();
    memcpy(temp, &p, sizeof(void*));
    temp = temp + sizeof(void*);
  }
  data_size = temp - buff;
  printf("directory size is : %zu", data_size);
}
template <typename K, typename V>
void RDMA_extensible_hash<K, V>::directory_deserialization() {
  char* buff = static_cast<char*>(local_directory_serialization_mr->addr);
  char* temp = buff;
  size_t namenumber_net;
  memcpy(&namenumber_net, temp, sizeof(size_t));
  size_t namenumber = htonl(namenumber_net);
  temp = temp + sizeof(size_t);

  char dbname_[namenumber+1];
  memcpy(dbname_, temp, namenumber);
  dbname_[namenumber] = '\0';
  temp = temp + namenumber;

  assert(db_name == std::string(dbname_));
  size_t bucketnumber_net;
  memcpy(&bucketnumber_net, temp, sizeof(size_t));
  size_t bucketnumber = htonl(bucketnumber_net);
  temp = temp + sizeof(size_t);
  std::map<void*, std::shared_ptr<Bucket>> bucket_map;
  void* p;

  for(size_t i = 0; i < bucketnumber; i++){

    memcpy(&p, temp, sizeof(void*));
    temp = temp + sizeof(void*);
    std::shared_ptr<Bucket> bucket = std::make_shared<Bucket>(0, bucketnumber, bucketSize);

    memcpy((void*)(bucket.get()), temp, sizeof(Bucket));
    temp = temp + sizeof(Bucket);
    bucket_recover_content(bucket);
    bucket_map.insert({p,bucket});
  }
  bucketDirectory.clear();
  int globalDepth_net;
  memcpy(&globalDepth_net, temp, sizeof(int));
  globalDepth = ntohl(globalDepth_net);
  temp = temp + sizeof(int);

  for(int i = 0; i< 1<<globalDepth; i++){
    memcpy(&p, temp, sizeof(void*));
    temp = temp + sizeof(void*);
    bucketDirectory.push_back(bucket_map.at(p));
  }

}
template <typename K, typename V>
void RDMA_extensible_hash<K, V>::realloc_remote_bucket_mr() {
//  rdma_mg_->remote_bucket_change(hash_id, globalDepth, bucketSize, remote_directory, db_name);
  printf("reallocating the remote bucket");
  if(remote_bucket_region)
    delete(remote_bucket_region);
  std::unique_lock<std::shared_mutex> l(rdma_mg_->main_qp_mutex);
  ibv_wc wc[4] = {};
  computing_to_memory_msg* send_pointer;
  send_pointer = (computing_to_memory_msg*)rdma_mg_->res->send_buf;
  send_pointer->command = remote_hash_region_change;
  send_pointer->content.bucket_change_command.hash_id = hash_id;
  send_pointer->content.bucket_change_command.global_depth = globalDepth;
  send_pointer->content.bucket_change_command.bucket_size = bucketSize;
  rdma_mg_->post_receive<int>(rdma_mg_->res->mr_receive, std::string("main"));


  // post the command for saving the serialized data.
  rdma_mg_->post_send<computing_to_memory_msg>(rdma_mg_->res->mr_send, std::string("main"));
  memcpy(rdma_mg_->res->send_buf, db_name.c_str(), db_name.size());
  memcpy(static_cast<char*>(rdma_mg_->res->send_buf)+db_name.size(), "\0", 1);
  rdma_mg_->post_receive<ibv_mr>(rdma_mg_->res->mr_receive, std::string("main"));
  rdma_mg_->post_send(rdma_mg_->res->mr_send,"main", db_name.size()+1);

  if (rdma_mg_->poll_completion(wc, 4, std::string("main"))) {
    fprintf(stderr, "failed to poll receive for serialized data size <retrieve>\n");
  }
  remote_bucket_region = new ibv_mr();
  *remote_bucket_region = *((ibv_mr*)rdma_mg_->res->receive_buf);
}
template <typename K, typename V>
void RDMA_extensible_hash<K, V>::directory_sync() {
  size_t data_size;
  directory_serialization(data_size);

  //TODO: the buffer for the directory may be overflow!
  assert(data_size < bucketSize);
  std::unique_lock<std::shared_mutex> l(rdma_mg_->main_qp_mutex);
  ibv_wc wc[2] = {};
  computing_to_memory_msg* send_pointer;
  send_pointer = (computing_to_memory_msg*)rdma_mg_->res->send_buf;
  send_pointer->command = hash_directory_update;
  send_pointer->content.dir_change.directory_size = 16*1024;

  rdma_mg_->post_receive<char>(rdma_mg_->res->mr_receive, std::string("main"));
  // post the command for saving the serialized data.
  rdma_mg_->post_send<computing_to_memory_msg>(rdma_mg_->res->mr_send, std::string("main"));
  if (!rdma_mg_->poll_completion(wc, 2, std::string("main"))) {
    memcpy(rdma_mg_->res->send_buf, db_name.c_str(), db_name.size());
    memcpy(static_cast<char*>(rdma_mg_->res->send_buf)+db_name.size(), "\0", 1);
    rdma_mg_->post_receive<char>(rdma_mg_->res->mr_receive, std::string("main"));
    rdma_mg_->post_send(rdma_mg_->res->mr_send,"main", db_name.size()+1);
  }else
    fprintf(stderr, "failed to poll receive for directory message\n");

  rdma_mg_->post_send(local_directory_serialization_mr, std::string("main"), data_size);


  if (!rdma_mg_->poll_completion(wc, 1, std::string("main")))
    printf("directory data sent successfully");
  else
    fprintf(stderr, "failed to poll send for directory data send\n");
}
template <typename K, typename V>
V* RDMA_extensible_hash<K, V>::at(K key) {
  V* result;
  this->find(key, result);
  return result;
}
template <typename K, typename V>
bool RDMA_extensible_hash<K, V>::Recover() {
  computing_to_memory_msg* send_pointer;
  send_pointer = (computing_to_memory_msg*)rdma_mg_->res->send_buf;
  send_pointer->command = retrieve_remote_hash;
  ibv_wc wc[2] = {};
  rdma_mg_->post_receive<char>(rdma_mg_->res->mr_receive, std::string("main"));
  // post the command for saving the serialized data.
  rdma_mg_->post_send<computing_to_memory_msg>(rdma_mg_->res->mr_send, std::string("main"));
  rdma_mg_->poll_completion(wc, 2, std::string("main"));
  memcpy( rdma_mg_->res->send_buf, db_name.c_str(), db_name.size());
  memcpy(static_cast<char*>( rdma_mg_->res->send_buf)+db_name.size(), "\0", 1);
  //receive the size of the serialized data
  rdma_mg_->post_receive<size_t>( rdma_mg_->res->mr_receive, std::string("main"));
  rdma_mg_->post_send( rdma_mg_->res->mr_send,"main", db_name.size()+1);
  rdma_mg_->poll_completion(wc, 2, std::string("main"));
  size_t buff_size = *reinterpret_cast<size_t*>(rdma_mg_->res->receive_buf);





  if (buff_size!=0 && buff_size <=1.2*bucketSize){
    rdma_mg_->post_receive(local_directory_serialization_mr,"main", buff_size);
    // send a char to tell the shared memory that this computing node is ready to receive the data
    rdma_mg_->post_send<char>(rdma_mg_->res->mr_send, std::string("main"));
    rdma_mg_->poll_completion(wc, 2, std::string("main"));
    directory_deserialization();
    return true;
  }
  else if (buff_size>12*bucketSize){
    printf("directory overflow");
    return false;
  }
  else
    return false;
}
template <typename K, typename V>
void RDMA_extensible_hash<K, V>::Bucket::bucket_serialization(char* buff,
                                                              size_t& size) {
  char* temp = buff;
  //TODO: Add data size at the begginning
  size_t element_num = contents->size();
  size_t element_num_net = htonl(element_num);
  memcpy(temp, &element_num_net, sizeof(size_t));
  temp = temp + sizeof(size_t);
  for (auto itr : *contents)
    itr.second->serialization(temp, size);
  size = temp - buff;


}
template <typename K, typename V>
void RDMA_extensible_hash<K, V>::Bucket::bucket_deserialization(
    char* buff,
                                                                size_t size) {
  char* temp = buff;
  size_t filenumber_net;
  memcpy(&filenumber_net, temp, sizeof(size_t));
  size_t filenumber = htonl(filenumber_net);
  temp = temp + sizeof(size_t);
  for (size_t i = 0; i < filenumber; i++){
    auto e = new V();
    K temp_key;
    e->deserialization(temp,size,temp_key);
    contents->insert({temp_key, e});
  }
}
template <typename K, typename V>
void RDMA_extensible_hash<K, V>::bucket_recover_content(std::shared_ptr<Bucket> bucket) {
  ibv_mr remote_mr;
  remote_mr = *remote_bucket_region;
  remote_mr.length = bucketSize;
  remote_mr.addr = static_cast<void*>(static_cast<char*>(remote_mr.addr) + bucketSize*bucket->bucket_num);
  rdma_mg_->RDMA_Read(&remote_mr, local_bucket_serialization_mr, bucketSize, "", IBV_SEND_SIGNALED,1);
  size_t dummy_size = 0;
  bucket->bucket_deserialization(static_cast<char*>(local_bucket_serialization_mr->addr), dummy_size);

}
}
#endif
