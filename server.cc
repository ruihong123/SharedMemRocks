
#include <iostream>
#include "rocksdb/rdma.h"


int main()
{
  auto* local = new std::unordered_map<ibv_mr*, In_Use_Array>();
  auto* remote = new std::unordered_map<ibv_mr*, In_Use_Array>();
  struct config_t config = {
      NULL,  /* dev_name */
      NULL,  /* server_name */
      19875, /* tcp_port */
      1,	 /* ib_port */
      -1, /* gid_idx */
  0};
  RDMA_Manager RDMA_manager(config, local, remote);
  RDMA_manager.Server_to_Client_Communication();


    return 0;
}
