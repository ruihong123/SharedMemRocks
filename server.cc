
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
  size_t block_size = 4*1024*1024;
  size_t table_size = 10*1024*1024;
  rocksdb::RDMA_Manager RDMA_manager(config, local, remote, block_size, 0,
                                     table_size, 0);
  RDMA_manager.Server_to_Client_Communication();


  return 0;
}
