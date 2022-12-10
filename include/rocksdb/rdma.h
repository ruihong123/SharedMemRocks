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

#include <cassert>

#include <algorithm>
#include <shared_mutex>
#include <thread>
#include <chrono>
#include <memory>
#include <sstream>



#include <arpa/inet.h>
#include <infiniband/verbs.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include "util/thread_local.h"
#include "util/mutexlock.h"
#include <atomic>
#include <chrono>
#include <iostream>
#include <map>
#include <vector>
#include <list>
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
enum RDMA_Command_Type {create_qp_, create_mr_,
  save_fs_serialized_data,
  retrieve_fs_serialized_data, save_log_serialized_data,
  retrieve_log_serialized_data
};
enum file_type {log_type, others};
struct fs_sync_command{
  int data_size;
  file_type type;
};
union RDMA_Command_Content{
  size_t mem_size;
  registered_qp_config qp_config;
  fs_sync_command fs_sync_cmd;
};

struct computing_to_memory_msg
{
  RDMA_Command_Type command;
  RDMA_Command_Content content;
} __attribute__((packed));

// Structure for the file handle in RDMA file system. it could be a link list
// for large files
struct SST_Metadata{
  std::shared_mutex file_lock;
  std::string fname;
  ibv_mr* mr;
  ibv_mr* map_pointer;
  SST_Metadata* last_ptr = nullptr;
  SST_Metadata* next_ptr = nullptr;
  unsigned int file_size = 0;

};
template <typename T>
struct atomwrapper
{
  std::atomic<T> _a;

  atomwrapper()
      :_a()
  {}

  atomwrapper(const std::atomic<T> &a)
      :_a(a.load())
  {}

  atomwrapper(const atomwrapper &other)
      :_a(other._a.load())
  {}

  atomwrapper &operator=(const atomwrapper &other)
  {
    _a.store(other._a.load());
  }
};


//class In_Use_Array{
// public:
//  In_Use_Array(size_t size, size_t chunk_size, ibv_mr* mr_ori)
//      : element_size_(size), chunk_size_(chunk_size), mr_ori_(mr_ori){
//    in_use_ = new std::atomic<bool>[element_size_];
//    for (size_t i = 0; i < element_size_; ++i){
//      in_use_[i] = false;
//    }
//
//  }
//  In_Use_Array(size_t size, size_t chunk_size, ibv_mr* mr_ori, std::atomic<bool>* in_use)
//      : element_size_(size), chunk_size_(chunk_size), in_use_(in_use), mr_ori_(mr_ori){
//
//  }
//  int allocate_memory_slot(){
//    for (int i = 0; i < static_cast<int>(element_size_); ++i){
////      auto start = std::chrono::high_resolution_clock::now();
//      bool temp = in_use_[i];
//      if (temp == false) {
////        auto stop = std::chrono::high_resolution_clock::now();
////        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
////        std::printf("Compare and swap time duration is %ld \n", duration.count());
//        if(in_use_[i].compare_exchange_strong(temp, true)){
////          std::cout << "chunk" <<i << "was changed to true" << std::endl;
//
//          return i; // find the empty slot then return the index for the slot
//
//        }
////        else
////          std::cout << "Compare and swap fail" << "i equals" << i  << "type is" << type_ << std::endl;
//      }
//
//    }
//    return -1; //Not find the empty memory chunk.
//  }
//  bool deallocate_memory_slot(int index) {
//    bool temp = true;
//    assert(in_use_[index] == true);
////    std::cout << "chunk" <<index << "was changed to false" << std::endl;
//
//    return in_use_[index].compare_exchange_strong(temp, false);
//
//  }
//  size_t get_chunk_size(){
//    return chunk_size_;
//  }
//  ibv_mr* get_mr_ori(){
//    return mr_ori_;
//  }
//  size_t get_element_size(){
//    return element_size_;
//  }
//  std::atomic<bool>* get_inuse_table(){
//    return in_use_;
//  }
////  void deserialization(char*& temp, int& size){
////
////
////  }
// private:
//  size_t element_size_;
//  size_t chunk_size_;
//  std::atomic<bool>* in_use_;
//  ibv_mr* mr_ori_;
////  int type_;
//};
class In_Use_Array {
 public:
  In_Use_Array(size_t size, size_t chunk_size, ibv_mr* mr_ori)
      : element_size_(size), chunk_size_(chunk_size), mr_ori_(mr_ori) {
    for (size_t i = 0; i < element_size_; ++i) {
      free_list.push_back(i);
    }
  }
  In_Use_Array(size_t size, size_t chunk_size, ibv_mr* mr_ori, bool no_init)
      : element_size_(size),
        chunk_size_(chunk_size),
        //        in_use_(in_use),
        mr_ori_(mr_ori) { assert(no_init);}
  int allocate_memory_slot() {
    std::unique_lock<rocksdb::SpinMutex> lck(mtx);
    if (free_list.empty())
      return -1;  // Not find the empty memory chunk.
    else{
      int result = free_list.back();
      free_list.pop_back();
      return result;
    }
  }
  bool deallocate_memory_slot(size_t index) {
    std::unique_lock<rocksdb::SpinMutex> lck(mtx);
    free_list.push_back(index);
    if (index < element_size_){
      return true;
    }else{
      assert(false);
      return false;
    }
  }
  size_t get_chunk_size() { return chunk_size_; }
  ibv_mr* get_mr_ori() { return mr_ori_; }
  size_t get_element_size() { return element_size_; }
  std::list<int>* get_free_list() { return &free_list; }
  //  void deserialization(char*& temp, int& size){
  //
  //
  //  }
 private:
  size_t element_size_;
  size_t chunk_size_;
  std::list<int> free_list;
  rocksdb::SpinMutex mtx;
  ibv_mr* mr_ori_;
  //  int type_;
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
struct IBV_Deleter {
  //Called by unique_ptr to destroy/free the Resource
  void operator()(ibv_mr* r) {
    if(r){
      void* pointer = r->addr;
      ibv_dereg_mr(r);
      free(pointer);
    }
  }
};
namespace ROCKSDB_NAMESPACE {
/* structure of test parameters */
class RDMA_Manager{
 public:
  RDMA_Manager(config_t config, std::map<void*, In_Use_Array*>* Remote_Bitmap,
               size_t table_size, std::string* db_name,
               std::unordered_map<std::string, SST_Metadata*>* file_to_sst_meta,
               std::shared_mutex* fs_mutex);
//  RDMA_Manager(config_t config) : rdma_config(config){
//    res = new resources();
//    res->sock = -1;
//  }
//  RDMA_Manager()=delete;
  ~RDMA_Manager();
  // RDMA set up create all the resources, and create one query pair for RDMA send & Receive.
  ibv_mr* Get_local_read_mr();
  void Client_Set_Up_Resources();
  //Set up the socket connection to remote shared memory.
  bool Client_Connect_to_Server_RDMA();
  // client function to retrieve serialized data.
  bool client_retrieve_serialized_data(const std::string& db_name, char*& buff,
                                       size_t& buff_size, ibv_mr*& local_mr,
                                       file_type type);
  // client function to save serialized data.
  bool client_save_serialized_data(const std::string& db_name, char* buff,
                                   size_t buff_size, file_type type,
                                   ibv_mr* local_mr);
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
  bool Preregister_Memory(int gb_number); //Pre register the memroy do not allocate bit map

  // Remote Memory registering will call RDMA send and receive to the remote memory
  // it also push the new SST bit map to the Remote_Mem_Bitmap
  bool Remote_Memory_Register(size_t size);
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
//    printf("Serialized data size: %zu", size_dummy);
    client_save_serialized_data(*db_name_, buff, size_dummy, log_type, nullptr);}
  //Allocate an empty remote SST, return the index for the memory slot
  void Allocate_Remote_RDMA_Slot(const std::string &file_name,
                                 SST_Metadata*& sst_meta);
  void Allocate_Local_RDMA_Slot(ibv_mr*& mr_input, ibv_mr*& map_pointer,
                                std::string pool_name);
  // this function will determine whether the pointer is with in the registered memory
  bool CheckInsideLocalBuff(void* p,
      std::_Rb_tree_iterator<std::pair<void* const, In_Use_Array*>>& mr_iter,
      std::map<void*, In_Use_Array*>* Bitmap);
  void mr_serialization(char*& temp, size_t& size, ibv_mr* mr);
  void mr_deserialization(char*& temp, size_t& size, ibv_mr*& mr);

  void fs_serialization(char*& buff, size_t& size, std::string& db_name,
      std::unordered_map<std::string, SST_Metadata*>& file_to_sst_meta,
      std::map<void*, In_Use_Array*>& remote_mem_bitmap);
  //Deserialization for linked file is problematic because different file may link to the same SSTdata
  void fs_deserilization(char*& buff, size_t& size, std::string& db_name,
      std::unordered_map<std::string, SST_Metadata*>& file_to_sst_meta,
      std::map<void*, In_Use_Array*>& remote_mem_bitmap,
                         ibv_mr* local_mr);
//  void mem_pool_serialization
    //TODO: Make all the variable more smart pointers.
  resources* res = nullptr;
  std::vector<ibv_mr*> remote_mem_pool; /* a vector for all the remote memory regions*/
  std::vector<ibv_mr*> local_mem_pool; /* a vector for all the local memory regions.*/
  std::list<ibv_mr*> pre_allocated_pool;
  std::map<void*, In_Use_Array*>* Remote_Mem_Bitmap = nullptr;
  uint64_t total_registered_size = 0;

#ifdef PROCESSANALYSIS
  static std::atomic<uint64_t> RDMAReadTimeElapseSum;
  static std::atomic<uint64_t> ReadCount;
#endif
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
  std::unordered_map<std::string, std::map<void*, In_Use_Array*>> name_to_mem_pool;
  std::unordered_map<std::string, size_t> name_to_size;
  std::shared_mutex local_mem_mutex;
  std::unordered_map<std::string, ibv_mr*> fs_image;
  std::unordered_map<std::string, ibv_mr*> log_image;
  ibv_mr* log_image_mr = nullptr;
  std::shared_mutex log_image_mutex;
  std::shared_mutex fs_image_mutex;
  ThreadLocalPtr* read_buffer;
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
  template <typename T>
  int post_send(ibv_mr* mr, std::string qp_id = "main");
  int post_send(ibv_mr* mr, std::string qp_id = "main", size_t size = 0);
//  int post_receives(int len);
  template <typename T>
  int post_receive(ibv_mr* mr, std::string qp_id = "main");
  int post_receive(ibv_mr* mr, std::string qp_id = "main", size_t size = 0);

  int resources_create();
  int modify_qp_to_init(struct ibv_qp* qp);
  int modify_qp_to_rtr(struct ibv_qp* qp, uint32_t remote_qpn, uint16_t dlid, uint8_t* dgid);
  int modify_qp_to_rts(struct ibv_qp* qp);
  bool create_qp(std::string& id);
  int connect_qp(registered_qp_config remote_con_data, std::string& qp_id);
  int resources_destroy();
  void print_config(void);
  void usage(const char* argv0);

  int post_receive(ibv_mr** mr_list, size_t sge_size, std::string qp_id);
  int post_send(ibv_mr** mr_list, size_t sge_size, std::string qp_id);
};
//class Hash_Map{
//  Hash_Map(size_t size, RDMA_Manager* rdma_mg, size_t bucket_size = 16*1024):
//            region_size_(size), bucket_size_(bucket_size), rdma_mg_(rdma_mg) {
//    void* temp_p = malloc(1*1024*1024);
//    int mr_flags =
//        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
//    mr = ibv_reg_mr(rdma_mg_->res->pd, static_cast<void*>(temp_p), region_size_, mr_flags);
//    memory_region = static_cast<char*>(mr->addr);
//  }
//  bool insert(std::string& filename, SST_Metadata* sst_p){
//    std::hash<std::string> hash;
//    size_t hash_value = hash(filename);
//    return false;
//  }
//  bool serialize_one_file(std::string& filename, SST_Metadata* sst_p, char* start_position, size_t size_delta){
//    char* temp_p = start_position;
//    size_t filename_length = filename.size();
//    size_t filename_length_net = htonl(filename_length);
//    memcpy(temp_p, &filename_length_net, sizeof(size_t));
//    temp_p = temp_p + sizeof(size_t);
//    memcpy(temp_p, filename.c_str(), filename_length);
//    temp_p = temp_p + filename_length;
//    unsigned int file_size = sst_p->file_size;
//    unsigned int file_size_net = htonl(file_size);
//    memcpy(temp_p, &file_size_net, sizeof(unsigned int));
//    temp_p = temp_p + sizeof(unsigned int);
//    // check how long is the list
//    SST_Metadata* meta_p = sst_p;
//    SST_Metadata* temp_meta = meta_p;
//    size_t list_len = 1;
//    while (temp_meta->next_ptr != nullptr) {
//      list_len++;
//      temp_meta = temp_meta->next_ptr;
//    }
//    size_t list_len_net = ntohl(list_len);
//    memcpy(temp_p, &list_len_net, sizeof(size_t));
//    temp_p = temp_p + sizeof(size_t);
//    meta_p = sst_p;
//    size_t length_map = meta_p->map_pointer->length;
//    size_t length_map_net = htonl(length_map);
//    memcpy(temp_p, &length_map_net, sizeof(size_t));
//    temp_p = temp_p + sizeof(size_t);
//    size_t dummy_size;
//    while (meta_p != nullptr) {
//
//      rdma_mg_->mr_serialization(temp_p, dummy_size, meta_p->mr);
//      // TODO: minimize the size of the serialized data. For exe, could we save
//      // TODO: the mr length only once?
//
//      void* p = meta_p->map_pointer->addr;
//      memcpy(temp_p, &p, sizeof(void*));
//      temp_p = temp_p + sizeof(void*);
//      meta_p = meta_p->next_ptr;
//
//
//    }
//    size_delta = temp_p - start_position;
//    return true;
//  }
//  bool search_in_bucket(std::string& filename, SST_Metadata* sst_p, char* start_position, size_t size_delta){
//    char* temp = start_position;
//    size_t filenumber_net;
//    memcpy(&filenumber_net, temp, sizeof(size_t));
//    size_t filenumber = htonl(filenumber_net);
//    temp = temp + sizeof(size_t);
//    for (size_t i = 0; i < filenumber; i++) {
//      size_t filename_length_net;
//      memcpy(&filename_length_net, temp, sizeof(size_t));
//      size_t filename_length = ntohl(filename_length_net);
//      temp = temp + sizeof(size_t);
//      char filename[filename_length+1];
//      memcpy(filename, temp, filename_length);
//      filename[filename_length] = '\0';
//      temp = temp + filename_length;
//
//      unsigned int file_size_net = 0;
//      memcpy(&file_size_net, temp, sizeof(unsigned int));
//      unsigned int file_size = ntohl(file_size_net);
//      temp = temp + sizeof(unsigned int);
//      size_t list_len_net = 0;
//      memcpy(&list_len_net, temp, sizeof(size_t));
//      size_t list_len = htonl(list_len_net);
//      temp = temp + sizeof(size_t);
//      SST_Metadata* meta_head;
//      SST_Metadata* meta = new SST_Metadata();
//      meta->file_size = file_size;
//      meta_head = meta;
//      size_t length_map_net = 0;
//      memcpy(&length_map_net, temp, sizeof(size_t));
//      size_t length_map = htonl(length_map_net);
//      temp = temp + sizeof(size_t);
//      for (size_t j = 0; j<list_len; j++){
//        //below could be problematic.
//        meta->fname = std::string(filename);
//        mr_deserialization(temp, size, meta->mr);
//        meta->map_pointer = new ibv_mr;
//        *(meta->map_pointer) = *(meta->mr);
//        void* start_key;
//        memcpy(&start_key, temp, sizeof(void*));
//        temp = temp + sizeof(void*);
//        meta->map_pointer->length = length_map;
//        meta->map_pointer->addr = start_key;
//        if (j!=list_len-1){
//          meta->next_ptr = new SST_Metadata();
//          meta = meta->next_ptr;
//        }
//
//      }
//      file_to_sst_meta.insert({std::string(filename), meta_head});
//    }
//  }
// private:
//  size_t region_size_;
//  size_t bucket_size_;
//  RDMA_Manager* rdma_mg_;
//  ibv_mr* mr;
//  char* memory_region;
//  size_t number_element = 0;
//};
//#ifdef __cplusplus
//}
//#endif
}
#endif
