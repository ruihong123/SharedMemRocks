
#include <iostream>
#include "rocksdb/rdma.h"


int main()
{
  struct config_t config = {
      NULL,  /* dev_name */
      NULL,  /* server_name */
      19875, /* tcp_port */
      1,	 /* ib_port */
      -1 /* gid_idx */ };
  RDMA_Manager RDMA_manager(config);
  RDMA_manager.Sever_thread();

  return 0;
}
