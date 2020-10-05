#include <iostream>
#include "rocksdb/rdma.h"


int main()
{
  struct config_t config = {
      NULL,  /* dev_name */
      NULL,  /* server_name */
      19875, /* tcp_port */
      1,	 /* ib_port */
      -1, /* gid_idx */
4*10*1024*1024 /*initial local SST buffer size*/
      };
  RDMA_Manager rdma_manager(config);
  rdma_manager.Set_Up_RDMA();
  rdma_manager.Remote_Memory_Register(1024*1024*1024);
  std::cout << rdma_manager.res->remote_mem_pool[0];
  ibv_mr mem_pool_table[2];
  mem_pool_table[0] = *(rdma_manager.res->local_mem_pool[0]);
  mem_pool_table[1] = *(rdma_manager.res->local_mem_pool[0]);
  mem_pool_table[1].addr = (void*)((char*)mem_pool_table[1].addr + sizeof("message from computing node"));// PROBLEM Could be here.

  char *msg = static_cast<char *>(rdma_manager.res->local_mem_pool[0]->addr);
  strcpy(msg, "message from computing node");
  int msg_size = sizeof("message from computing node");
  rdma_manager.RDMA_Write(rdma_manager.res->remote_mem_pool[0],&mem_pool_table[0], msg_size);
  ibv_wc* wc = new ibv_wc();
  while(wc->opcode != IBV_WC_RDMA_WRITE){
    rdma_manager.poll_completion(wc);
    if (wc->status != 0){
      fprintf(stderr, "Work completion status is %d \n", wc->status);
      fprintf(stderr, "Work completion opcode is %d \n", wc->opcode);
    }

  }
  rdma_manager.RDMA_Read(rdma_manager.res->remote_mem_pool[0],&mem_pool_table[1], msg_size);
  std::cout << "write buffer: " << (char*)rdma_manager.res->local_mem_pool[0]->addr << std::endl;

  std::cout << "read buffer: " << (char*)rdma_manager.res->local_mem_pool[1]->addr << std::endl;

  return 0;
}
