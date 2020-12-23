
#include <iostream>
#include "rocksdb/rdma.h"


int main()
{
  auto Remote_Bitmap = new std::map<void*, In_Use_Array>;
  auto Read_Bitmap = new std::map<void*, In_Use_Array>;
  auto Write_Bitmap = new std::map<void*, In_Use_Array>;
  std::string db_name;
  std::unordered_map<std::string, SST_Metadata*> file_to_sst_meta;
  std::shared_mutex fs_mutex;
  struct config_t config = {
      NULL,  /* dev_name */
      NULL,  /* server_name */
      19875, /* tcp_port */
      1,	 /* ib_port */
      1, /* gid_idx */
  0};
//  size_t write_block_size = 4*1024*1024;
//  size_t read_block_size = 4*1024;
  size_t table_size = 10*1024*1024;
  rocksdb::RDMA_Manager  RDMA_manager(config, Remote_Bitmap, table_size, &db_name,
                             &file_to_sst_meta,
                             &fs_mutex);

  RDMA_manager.Server_to_Client_Communication();


  return 0;
}
