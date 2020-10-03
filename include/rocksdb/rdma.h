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
//#include <options.h>

#include <sys/time.h>
#include <arpa/inet.h>
#include <infiniband/verbs.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
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
#define RDMAMSGR "RDMA read operation "
#define RDMAMSGW "RDMA write operation"
//#define MSG_SIZE (1024*1024)
#if __BYTE_ORDER == __LITTLE_ENDIAN
	static inline uint64_t htonll(uint64_t x) { return bswap_64(x); }
	static inline uint64_t ntohll(uint64_t x) { return bswap_64(x); }
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

/* structure of system resources */
struct resources
{
  struct ibv_device_attr
      device_attr;
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
  struct ibv_mr* mr_SST = nullptr;                        /* MR handle for SST_buf */
//  struct ibv_mr* mr_remote;                     /* remote MR handle for computing node */
  char* SST_buf = nullptr;			/* SSTable buffer pools pointer, it could contain multiple SSTbuffers */
  char* send_buf = nullptr;                       /* SEND buffer pools pointer, it could contain multiple SEND buffers */
  char* receive_buf = nullptr;		        /* receive buffer pool pointer,  it could contain multiple acturall receive buffers */
  std::vector<ibv_mr*> remote_mem_pool; /* a vector for all the remote memory regions*/
  int sock;						   /* TCP socket file descriptor */
};
/* structure of test parameters */
class RDMA_Manager{
 public:
  RDMA_Manager(config_t config);
  ~RDMA_Manager();

  void Set_Up_RDMA();
  bool Local_Memory_Register(char* buff,ibv_mr* mr, size_t size);// register the memory on the local side
  bool Remote_Memory_Register(size_t size);
  int Remote_Memory_Deregister();
  void Sever_thread();
  int RDMA_Read(ibv_mr* remote_mr, ibv_mr* local_mr, size_t msg_size);
  int RDMA_Write(ibv_mr* remote_mr, ibv_mr* local_mr, size_t msg_size);
  int RDMA_Send();
  resources* res = nullptr;
 private:
  config_t rdma_config;

  int sock_connect(const char* servername, int port);
  int sock_sync_data(int sock, int xfer_size, char* local_data, char* remote_data);
  int poll_completion(ibv_wc &wc);
  int post_send(void* mr, bool is_server);
//  int post_receives(int len);
  int post_receive(void* mr, bool is_server);

  int resources_create(size_t buffer_size);
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