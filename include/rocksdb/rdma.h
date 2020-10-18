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
#include <getopt.h>
#include <cassert>
#include <unordered_map>
#include <algorithm>
#include <shared_mutex>
//#include <options.h>

#include <arpa/inet.h>
#include <infiniband/verbs.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>

#include <atomic>
#include <chrono>
#include <iostream>
#include <vector>
//#ifdef __cplusplus
//extern "C" { //only need to export C interface if
//// used by C++ source code
//#endif

/* poll CQ timeout in millisec (2 seconds) */
#define MAX_POLL_CQ_TIMEOUT 1000000
#define MSG "SEND operation "

#if __BYTE_ORDER == __LITTLE_ENDIAN
//	static inline uint64_t htonll(uint64_t x) { return bswap_64(x); }
//	static inline uint64_t ntohll(uint64_t x) { return bswap_64(x); }
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
  int ib_port;		  /* local IB port to work with */
  int gid_idx;		  /* gid index to use */
  int init_local_buffer_size;   /*initial local SST buffer size*/
};
struct computing_to_memory_msg
{
  size_t mem_size;


};
/* structure to exchange data which is needed to connect the QPs */
struct registered_qp_config {
  uint32_t qp_num; /* QP number */
  uint16_t lid;	/* LID of the IB port */
  uint8_t gid[16]; /* gid */
} __attribute__((packed));
// Structure for the file handle in RDMA file system.
struct SST_Metadata{
  std::shared_mutex file_lock;
  std::string fname;
  ibv_mr* mr;
  ibv_mr* map_pointer;
  ibv_mr* last_ptr;
  ibv_mr* next_ptr;
  size_t file_size = 0;

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
class In_Use_Array{
 public:
  In_Use_Array(size_t size) : size_(size){
    in_use = new std::atomic<bool>[size_];
    for (size_t i = 0; i < size_; ++i){
      in_use[i] = false;
    }
  }
  int allocate_memory_slot(){
    for (int i = 0; i < static_cast<int>(size_); ++i){
      bool temp = in_use[i];
      if (temp == false) {
        if(in_use[i].compare_exchange_strong(temp, true))
          return i; // find the empty slot then return the index for the slot
      }

    }
    return -1; //Not find the empty memory chunk.
  }
  bool deallocate_memory_slot(int index) {
    bool temp = true;

    return in_use[index].compare_exchange_strong(temp, false);

  }
 private:
  size_t size_;
  std::atomic<bool>* in_use;
};
/* structure of system resources */
struct resources
{
  struct ibv_device_attr device_attr;
  /* Device attributes */
  struct ibv_sge* sge = nullptr;
  struct ibv_recv_wr*	rr = nullptr;
  struct ibv_port_attr port_attr;	/* IB port attributes */
//  std::vector<registered_qp_config> remote_mem_regions; /* memory buffers for RDMA */
  struct ibv_context* ib_ctx = nullptr;		   /* device handle */
  struct ibv_pd* pd = nullptr;				   /* PD handle */
  struct ibv_cq* cq = nullptr;				   /* CQ handle */
  struct ibv_qp* qp = nullptr;				   /* QP handle */
  struct ibv_mr* mr_receive = nullptr;              /* MR handle for receive_buf */
  struct ibv_mr* mr_send = nullptr;                 /* MR handle for send_buf */
//  struct ibv_mr* mr_SST = nullptr;                        /* MR handle for SST_buf */
//  struct ibv_mr* mr_remote;                     /* remote MR handle for computing node */
  char* SST_buf = nullptr;			/* SSTable buffer pools pointer, it could contain multiple SSTbuffers */
  char* send_buf = nullptr;                       /* SEND buffer pools pointer, it could contain multiple SEND buffers */
  char* receive_buf = nullptr;		        /* receive buffer pool pointer,  it could contain multiple acturall receive buffers */

  int sock;						   /* TCP socket file descriptor */
};
/* structure of test parameters */
class RDMA_Manager{
 public:
  RDMA_Manager(config_t config, std::unordered_map<ibv_mr*,
               In_Use_Array>* Remote_Bitmap,
               std::unordered_map<ibv_mr*, In_Use_Array>* Local_Bitmap);
//  RDMA_Manager(config_t config) : rdma_config(config){
//    res = new resources();
//    res->sock = -1;
//  }
  RDMA_Manager()=delete;
  ~RDMA_Manager();

  void Set_Up_RDMA();
  // Local memory register need to first allocate memory outside them register it.
  // it also push the new block bit map to the Remote_Mem_Bitmap
  bool Local_Memory_Register(char** p2buffpointer, ibv_mr** p2mrpointer, size_t size);// register the memory on the local side
  // Remote Memory registering will call RDMA send and receive to the remote memory
  // it also push the new SST bit map to the Remote_Mem_Bitmap
  bool Remote_Memory_Register(size_t size);
  int Remote_Memory_Deregister();
  void Sever_thread();
  int RDMA_Read(ibv_mr* remote_mr, ibv_mr* local_mr, size_t msg_size);
  int RDMA_Write(ibv_mr* remote_mr, ibv_mr* local_mr, size_t msg_size);
  int RDMA_Send();
  int poll_completion(ibv_wc* wc_p, int num_entries);
  bool Deallocate_Local_RDMA_Slot(ibv_mr* mr, ibv_mr* map_pointer) const;
  bool Deallocate_Remote_RDMA_Slot(SST_Metadata* sst_meta) const;

  //Allocate an empty remote SST, return the index for the memory slot
  void Allocate_Remote_RDMA_Slot(const std::string &file_name,
                                 SST_Metadata*& sst_meta);
  void Allocate_Local_RDMA_Slot(ibv_mr*& mr_input, ibv_mr*& map_pointer);

  resources* res = nullptr;
  std::vector<ibv_mr*> remote_mem_pool; /* a vector for all the remote memory regions*/
  std::vector<ibv_mr*> local_mem_pool; /* a vector for all the local memory regions, which is mainly designed for Shared memory side*/
  std::unordered_map<ibv_mr*, In_Use_Array>* Remote_Mem_Bitmap = nullptr;
  std::unordered_map<ibv_mr*, In_Use_Array>* Local_Mem_Bitmap = nullptr;
  size_t Block_Size = 4*1024;
  size_t Table_Size = 4*1080*1024;
  std::mutex create_mutex;
 private:

  config_t rdma_config;

  int sock_connect(const char* servername, int port);
  int sock_sync_data(int sock, int xfer_size, char* local_data, char* remote_data);

  int post_send(void* mr, bool is_server);// should change it into <template>post_send(void* mr, template &send_data)
//  int post_receives(int len);
  int post_receive(void* mr, bool is_server);

  int resources_create();
  int modify_qp_to_init(struct ibv_qp* qp);
  int modify_qp_to_rtr(struct ibv_qp* qp, uint32_t remote_qpn, uint16_t dlid, uint8_t* dgid);
  int modify_qp_to_rts(struct ibv_qp* qp);
  int connect_qp();
  int resources_destroy();
  void print_config(void);
  void usage(const char* argv0);

};

//#ifdef __cplusplus
//}
//#endif

#endif