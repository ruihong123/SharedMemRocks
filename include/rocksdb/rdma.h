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
  char* server_name;	/* server host name */
  u_int32_t tcp_port;   /* server TCP port */
  int ib_port;		  /* local IB port to work with */
  int gid_idx;		  /* gid index to use */
};
/* structure to exchange data which is needed to connect the QPs */
struct registered_mem_config
{
  uint64_t addr;   /* Buffer address */
  uint32_t rkey;   /* Remote key */
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
  struct ibv_sge* sge;
  struct ibv_recv_wr*	rr;
  struct ibv_port_attr port_attr;	/* IB port attributes */
  std::vector<registered_mem_config> mem_regions; /* memory buffers for RDMA */
  struct ibv_context* ib_ctx;		   /* device handle */
  struct ibv_pd* pd;				   /* PD handle */
  struct ibv_cq* cq;				   /* CQ handle */
  struct ibv_qp* qp;				   /* QP handle */
  struct ibv_mr* mr;				   /* MR handle for buf */
  char* buf;						   /* memory buffer pointer, used for RDMA and send
	ops */
  int sock;						   /* TCP socket file descriptor */
};
/* structure of test parameters */
class RDMA_Manager{
 public:
  RDMA_Manager(config_t config);
  ~RDMA_Manager();

  void set_up_RDMA();

 private:
  config_t rdma_config;
  resources* res;
  int sock_connect(const char* servername, int port);
  int sock_sync_data(int sock, int xfer_size, char* local_data, char* remote_data);
  int poll_completion();
  int post_send(int opcode);
  int post_receives(int len);
  int post_receive();
  void resources_init();
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