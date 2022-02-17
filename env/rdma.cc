#include <include/rocksdb/rdma.h>
namespace ROCKSDB_NAMESPACE {
#ifdef PROCESSANALYSIS
std::atomic<uint64_t> RDMA_Manager::RDMAReadTimeElapseSum = 0;
std::atomic<uint64_t> RDMA_Manager::ReadCount = 0;

#endif
void UnrefHandle_rdma(void* ptr){
  delete static_cast<std::string*>(ptr);
}
void UnrefHandle_qp(void* ptr){
  if (ptr == nullptr)
    return;
  if (ibv_destroy_qp(static_cast<ibv_qp*>(ptr))) {
    fprintf(stderr, "Thread local qp failed to destroy QP\n");
  }
  else{
#ifndef NDEBUG
    printf("thread local qp destroy successfully!");
#endif
  }
}
void UnrefHandle_cq(void* ptr){
  if (ptr == nullptr)
    return;
  if (ibv_destroy_cq(static_cast<ibv_cq*>(ptr))) {
    fprintf(stderr, "Thread local cq failed to destroy QP\n");
  }
  else{
#ifndef NDEBUG
    printf("thread local cq destroy successfully!");
#endif
  }
}
/******************************************************************************
* Function: RDMA_Manager

*
* Output
* none
*
*
* Description
* Initialize the resource for RDMA.
******************************************************************************/
RDMA_Manager::RDMA_Manager(
    config_t config, std::map<void*, In_Use_Array*>* Remote_Bitmap,
    size_t table_size, std::string* db_name,
    std::unordered_map<std::string, SST_Metadata*>* file_to_sst_meta,
    std::shared_mutex* fs_mutex)
    : Table_Size(table_size),
      t_local_1(new ThreadLocalPtr(&UnrefHandle_rdma)),
      qp_local(new ThreadLocalPtr(&UnrefHandle_qp)),
      cq_local(new ThreadLocalPtr(&UnrefHandle_cq)),
      rdma_config(config),
      db_name_(db_name),
      file_to_sst_meta_(file_to_sst_meta),
      fs_mutex_(fs_mutex)

{
//  assert(read_block_size <table_size);
  res = new resources();
  printf("initialize the RDMA manager\n");
  //  res->sock = -1;
  Remote_Mem_Bitmap = Remote_Bitmap;
//  Write_Local_Mem_Bitmap = Write_Bitmap;
//  Read_Local_Mem_Bitmap = Read_Bitmap;
}
/******************************************************************************
* Function: ~RDMA_Manager

*
* Output
* none
*
*
* Description
* Cleanup and deallocate all resources used for RDMA
******************************************************************************/
RDMA_Manager::~RDMA_Manager() {
  if (!res->qp_map.empty())
    for (auto it = res->qp_map.begin(); it != res->qp_map.end(); it++) {
      if (ibv_destroy_qp(it->second)) {
        fprintf(stderr, "failed to destroy QP\n");
      }
    }
  delete qp_local;
  delete  cq_local;
  delete t_local_1;
#ifdef PROCESSANALYSIS
  if (RDMA_Manager::ReadCount.load() != 0)
    printf("RDMA read operatoion average time duration: %zu, ReadNuM is%zu, "
        "total time is %zu\n",
        RDMA_Manager::RDMAReadTimeElapseSum.load()/RDMA_Manager::ReadCount.load(),
        RDMA_Manager::ReadCount.load(), RDMA_Manager::RDMAReadTimeElapseSum.load());
  else
    printf("No RDMA read recorded\n");
#endif
//  for (auto & iter : name_to_mem_pool){
//    delete iter.second;
//  }
//  if (res->mr_receive)
//    if (ibv_dereg_mr(res->mr_receive)) {
//      fprintf(stderr, "failed to deregister MR\n");
//    }
//  if (res->mr_send)
//    if (ibv_dereg_mr(res->mr_send)) {
//      fprintf(stderr, "failed to deregister MR\n");
//    }
  if (!local_mem_pool.empty()) {
    //    ibv_dereg_mr(local_mem_pool.at(0));
    //    std::for_each(local_mem_pool.begin(), local_mem_pool.end(), ibv_dereg_mr);
    for (ibv_mr* p : local_mem_pool) {
      ibv_dereg_mr(p);
      //       local buffer is registered on this machine need deregistering.
      delete (char*)p->addr;
    }
    //    local_mem_pool.clear();
  }
  if (log_image_mr != nullptr){
    ibv_dereg_mr(log_image_mr);
    delete (char*)log_image_mr->addr;
  }
  if (!remote_mem_pool.empty()) {
    for (auto p : remote_mem_pool) {
      delete p;  // remote buffer is not registered on this machine so just delete the structure
    }
    remote_mem_pool.clear();
  }
//  if (res->receive_buf) delete res->receive_buf;
//  if (res->send_buf) delete res->send_buf;
  //  if (res->SST_buf)
  //    delete res->SST_buf;
  if (!res->cq_map.empty())
    for (auto it = res->cq_map.begin(); it != res->cq_map.end(); it++) {
      if (ibv_destroy_cq(it->second)) {
        fprintf(stderr, "failed to destroy CQ\n");
      }
    }

  if (res->pd)
    if (ibv_dealloc_pd(res->pd)) {
      fprintf(stderr, "failed to deallocate PD\n");
    }

  if (res->ib_ctx)
    if (ibv_close_device(res->ib_ctx)) {
      fprintf(stderr, "failed to close device context\n");
    }
  if (!res->sock_map.empty())
    for (auto it = res->sock_map.begin(); it != res->sock_map.end(); it++) {
      if (close(it->second)) {
        fprintf(stderr, "failed to close socket\n");
      }
    }
  for (auto pool : name_to_mem_pool) {
    for(auto iter : pool.second){
      delete iter.second;
    }
  }
  //TODO: Understand why the code below will get stuck.
//  for(auto iter : *Remote_Mem_Bitmap){
//    delete iter.second;
//  }
  delete Remote_Mem_Bitmap;
  delete res;
}
/******************************************************************************
Socket operations
For simplicity, the example program uses TCP sockets to exchange control
information. If a TCP/IP stack/connection is not available, connection manager
(CM) may be used to pass this information. Use of CM is beyond the scope of
this example
******************************************************************************/
/******************************************************************************
* Function: sock_connect
*
* Input
* servername URL of server to connect to (NULL for server mode)
* port port of service
*
* Output
* none
*
* Returns
* socket (fd) on success, negative error code on failure
*
* Description
* Connect a socket. If servername is specified a client connection will be
* initiated to the indicated server and port. Otherwise listen on the
* indicated port for an incoming connection.
*
******************************************************************************/
int RDMA_Manager::client_sock_connect(const char* servername, int port) {
  struct addrinfo* resolved_addr = NULL;
  struct addrinfo* iterator;
  char service[6];
  int sockfd = -1;
  int listenfd = 0;
  int tmp;
  struct addrinfo hints = {
      .ai_flags = AI_PASSIVE, .ai_family = AF_INET, .ai_socktype = SOCK_STREAM};
  if (sprintf(service, "%d", port) < 0) goto sock_connect_exit;
  /* Resolve DNS address, use sockfd as temp storage */
  sockfd = getaddrinfo(servername, service, &hints, &resolved_addr);
  if (sockfd < 0) {
    fprintf(stderr, "%s for %s:%d\n", gai_strerror(sockfd), servername, port);
    goto sock_connect_exit;
  }
  /* Search through results and find the one we want */
  for (iterator = resolved_addr; iterator; iterator = iterator->ai_next) {
    sockfd = socket(iterator->ai_family, iterator->ai_socktype,
                    iterator->ai_protocol);
    if (sockfd >= 0) {
      if (servername) {
        /* Client mode. Initiate connection to remote */
        if ((tmp = connect(sockfd, iterator->ai_addr, iterator->ai_addrlen))) {
          fprintf(stdout, "failed connect \n");
          close(sockfd);
          sockfd = -1;
        }
      } else {
        /* Server mode. Set up listening socket an accept a connection */
        listenfd = sockfd;
        sockfd = -1;
        if (bind(listenfd, iterator->ai_addr, iterator->ai_addrlen))
          goto sock_connect_exit;
        listen(listenfd, 1);
        sockfd = accept(listenfd, NULL, 0);
      }
    }
    fprintf(stdout, "TCP connection was established\n");

  }
  sock_connect_exit:
  if (listenfd) close(listenfd);
  if (resolved_addr) freeaddrinfo(resolved_addr);
  if (sockfd < 0) {
    if (servername)
      fprintf(stderr, "Couldn't connect to %s:%d\n", servername, port);
    else {
      perror("server accept");
      fprintf(stderr, "accept() failed\n");
    }
  }
  return sockfd;
}
// connection code for server side, will get prepared for multiple connection
// on the same port.
int RDMA_Manager::server_sock_connect(const char* servername, int port) {
  struct addrinfo* resolved_addr = NULL;
  struct addrinfo* iterator;
  char service[6];
  int sockfd = -1;
  int listenfd = 0;
  struct sockaddr address;
  socklen_t len = sizeof(struct sockaddr);
  struct addrinfo hints = {
      .ai_flags = AI_PASSIVE, .ai_family = AF_INET, .ai_socktype = SOCK_STREAM};
  if (sprintf(service, "%d", port) < 0) goto sock_connect_exit;
  /* Resolve DNS address, use sockfd as temp storage */
  sockfd = getaddrinfo(servername, service, &hints, &resolved_addr);
  if (sockfd < 0) {
    fprintf(stderr, "%s for %s:%d\n", gai_strerror(sockfd), servername, port);
    goto sock_connect_exit;
  }

  /* Search through results and find the one we want */
  for (iterator = resolved_addr; iterator; iterator = iterator->ai_next) {
    sockfd = socket(iterator->ai_family, iterator->ai_socktype,
                    iterator->ai_protocol);
    int option = 1;
    setsockopt(sockfd,SOL_SOCKET,SO_REUSEADDR,&option,sizeof(int));
    if (sockfd >= 0) {
      /* Server mode. Set up listening socket an accept a connection */
      listenfd = sockfd;
      sockfd = -1;
      if (bind(listenfd, iterator->ai_addr, iterator->ai_addrlen))
        goto sock_connect_exit;
      listen(listenfd, 20);
      while (1) {
        sockfd = accept(listenfd, &address, &len);
        std::string client_id = std::string(inet_ntoa(((struct sockaddr_in*)(&address))->sin_addr))
                                + std::to_string(((struct sockaddr_in*)(&address))->sin_port);
        //Client id must be composed of ip address and port number.
        std::cout << "connection built up from" << client_id << std::endl;
        std::cout << "connection family is " << address.sa_family << std::endl;
        if (sockfd < 0) {
          fprintf(stderr, "Connection accept error, erron: %d\n", errno);
          break;
        }
        thread_pool.push_back(std::thread(
            [this](std::string client_ip, int socketfd) {
              this->server_communication_thread(client_ip, socketfd);
            },
            std::string(address.sa_data), sockfd));
//        thread_pool.back().detach();
      }
    }
  }
  sock_connect_exit:

  if (listenfd) close(listenfd);
  if (resolved_addr) freeaddrinfo(resolved_addr);
  if (sockfd < 0) {
    if (servername)
      fprintf(stderr, "Couldn't connect to %s:%d\n", servername, port);
    else {
      perror("server accept");
      fprintf(stderr, "accept() failed\n");
    }
  }
  return sockfd;
}
void RDMA_Manager::server_communication_thread(std::string client_ip,
                                               int socket_fd) {
  printf("A new shared memory thread start\n");
  printf("checkpoint1");
  char temp_receive[2];
  char temp_send[] = "Q";
  struct registered_qp_config local_con_data;
  struct registered_qp_config remote_con_data;
  struct registered_qp_config tmp_con_data;
  //  std::string qp_id = "main";
  int rc = 0;



  /* exchange using TCP sockets info required to connect QPs */
  printf("checkpoint1");
  create_qp(client_ip);
  local_con_data.qp_num = htonl(res->qp_map[client_ip]->qp_num);
  local_con_data.lid = htons(res->port_attr.lid);
  memcpy(local_con_data.gid, &res->my_gid, 16);
  printf("checkpoint2");

  fprintf(stdout, "\nLocal LID = 0x%x\n", res->port_attr.lid);
  if (sock_sync_data(socket_fd, sizeof(struct registered_qp_config),
                     (char*)&local_con_data, (char*)&tmp_con_data) < 0) {
    fprintf(stderr, "failed to exchange connection data between sides\n");
    rc = 1;
  }
  remote_con_data.qp_num = ntohl(tmp_con_data.qp_num);
  remote_con_data.lid = ntohs(tmp_con_data.lid);
  memcpy(remote_con_data.gid, tmp_con_data.gid, 16);
  fprintf(stdout, "Remote QP number = 0x%x\n", remote_con_data.qp_num);
  fprintf(stdout, "Remote LID = 0x%x\n", remote_con_data.lid);
  if (connect_qp(remote_con_data, client_ip)) {
    fprintf(stderr, "failed to connect QPs\n");
  }

  ibv_mr* send_mr;
  char* send_buff;
  if (!Local_Memory_Register(&send_buff, &send_mr, 1000, std::string())) {
    fprintf(stderr, "memory registering failed by size of 0x%x\n", 1000);
  }
  ibv_mr* recv_mr;
  char* recv_buff;

  if (!Local_Memory_Register(&recv_buff, &recv_mr, 1000, std::string())) {
    fprintf(stderr, "memory registering failed by size of 0x%x\n", 1000);
  }

//  post_receive<int>(recv_mr, client_ip);
  post_receive<computing_to_memory_msg>(recv_mr, client_ip);
  local_mem_pool.reserve(100);
  {
    std::unique_lock<std::shared_mutex> lck(local_mem_mutex);
    Preregister_Memory(88);
  }

  // sync after send & recv buffer creation and receive request posting.
  if (sock_sync_data(socket_fd, 1, temp_send,
                     temp_receive)) /* just send a dummy char back and forth */
  {
    fprintf(stderr, "sync error after QPs are were moved to RTS\n");
    rc = 1;
  }
  shutdown(socket_fd, 2);
  close(socket_fd);
//  post_send<int>(res->mr_send, client_ip);
  ibv_wc wc[3] = {};
//  if(poll_completion(wc, 2, client_ip))
//    printf("The main qp not create correctly");
//  else
//    printf("The main qp not create correctly");
  // Computing node and share memory connection succeed.
  // Now is the communication through rdma.
  computing_to_memory_msg receive_msg_buf;

//  receive_msg_buf = (computing_to_memory_msg*)recv_buff;
  //  receive_msg_buf->command = ntohl(receive_msg_buf->command);
  //  receive_msg_buf->content.qp_config.qp_num = ntohl(receive_msg_buf->content.qp_config.qp_num);
  //  receive_msg_buf->content.qp_config.lid = ntohs(receive_msg_buf->content.qp_config.lid);
//  ibv_wc wc[3] = {};
  //TODO: implement a heart beat mechanism.
  while (true) {
    poll_completion(wc, 1, client_ip);
    printf("receive one RPC via RDMA\n");
    memcpy(&receive_msg_buf, recv_buff, sizeof(computing_to_memory_msg));
    // copy the pointer of receive buf to a new place because
    // it is the same with send buff pointer.
    if (receive_msg_buf.command == create_mr_) {
      std::cout << "create memory region command receive for" << client_ip
                << std::endl;
      ibv_mr* send_pointer = (ibv_mr*)send_buff;
      ibv_mr* mr;
      char* buff;
      auto start = std::chrono::high_resolution_clock::now();
      if (!Local_Memory_Register(&buff, &mr, receive_msg_buf.content.mem_size,
                                 std::string())) {
        fprintf(stderr, "memory registering failed by size of 0x%x\n",
                static_cast<unsigned>(receive_msg_buf.content.mem_size));
      }
      auto stop = std::chrono::high_resolution_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
      std::printf("RDMA Memory registeration size: %zu time elapse (%ld) us\n", receive_msg_buf.content.mem_size, duration.count());

      *send_pointer = *mr;
      post_receive<computing_to_memory_msg>(recv_mr, client_ip);
      post_send<ibv_mr>(
          send_mr,
          client_ip);  // note here should be the mr point to the send buffer.
      poll_completion(wc, 1, client_ip);
    } else if (receive_msg_buf.command == create_qp_) {
      char gid_str[17];
      memset(gid_str,0,17);
      memcpy(gid_str, receive_msg_buf.content.qp_config.gid, 16);
      std::string new_qp_id =
          std::string(gid_str)+
          std::to_string(receive_msg_buf.content.qp_config.lid) +
          std::to_string(receive_msg_buf.content.qp_config.qp_num);
      std::cout << "create query pair command receive for" << client_ip
                << std::endl;
      fprintf(stdout, "Remote QP number=0x%x\n",
              receive_msg_buf.content.qp_config.qp_num);
      fprintf(stdout, "Remote LID = 0x%x\n",
              receive_msg_buf.content.qp_config.lid);
      registered_qp_config* send_pointer = (registered_qp_config*)send_buff;
      create_qp(new_qp_id);
      if (rdma_config.gid_idx >= 0) {
        rc = ibv_query_gid(res->ib_ctx, rdma_config.ib_port,
                           rdma_config.gid_idx, &(res->my_gid));
        if (rc) {
          fprintf(stderr, "could not get gid for port %d, index %d\n",
                  rdma_config.ib_port, rdma_config.gid_idx);
          return;
        }
      } else
        memset(&(res->my_gid), 0, sizeof (res->my_gid));
      /* exchange using TCP sockets info required to connect QPs */
      send_pointer->qp_num = res->qp_map[new_qp_id]->qp_num;
      send_pointer->lid = res->port_attr.lid;
      memcpy(send_pointer->gid, &(res->my_gid), 16);
      connect_qp(receive_msg_buf.content.qp_config, new_qp_id);
      post_receive<computing_to_memory_msg>(recv_mr, client_ip);
      post_send<registered_qp_config>(send_mr, client_ip);
      poll_completion(wc, 1, client_ip);
    }else if (receive_msg_buf.command == retrieve_fs_serialized_data){
      printf("retrieve_fs_serialized_data message received successfully\n");
      post_receive(recv_mr,client_ip, 1000);
      post_send<int>(send_mr,client_ip);
      // prepare the receive for db name, the name should not exceed 1000byte

      poll_completion(wc, 2, client_ip);
      std::string dbname;
      // Here could be some problem.
      dbname = std::string(recv_buff);
      std::cout << "retrieve db_name is: " << dbname <<std::endl;
      ibv_mr* local_mr;
      std::shared_lock<std::shared_mutex> l(fs_image_mutex);
      if (fs_image.find(dbname)!= fs_image.end()){
        local_mr = fs_image.at(dbname);
        l.unlock();
        *(reinterpret_cast<size_t*>(send_buff)) = local_mr->length;
        post_send<size_t>(send_mr,client_ip);
        post_receive<char>(recv_mr,client_ip);
        poll_completion(wc, 2, client_ip);
        post_receive<computing_to_memory_msg>(recv_mr, client_ip);
        post_send(local_mr,client_ip, local_mr->length);
        poll_completion(wc, 1, client_ip);
      }else{
        l.unlock();
        *(reinterpret_cast<size_t*>(send_buff)) = 0;
        post_receive<computing_to_memory_msg>(recv_mr, client_ip);
        post_send<size_t>(send_mr,client_ip);
        poll_completion(wc, 1, client_ip);
      }


    }else if (receive_msg_buf.command == save_fs_serialized_data){
      printf("save_fs_serialized_data message received successfully\n");
      int buff_size = receive_msg_buf.content.fs_sync_cmd.data_size;
      file_type filetype = receive_msg_buf.content.fs_sync_cmd.type;

      char* buff = static_cast<char*>(malloc(buff_size));
      ibv_mr* local_mr;
      int mr_flags =
          IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
      local_mr = ibv_reg_mr(res->pd, static_cast<void*>(buff), buff_size, mr_flags);
      post_receive(local_mr,client_ip, buff_size);
      post_receive<computing_to_memory_msg>(recv_mr, client_ip);
      post_send<char>(recv_mr,client_ip);
      poll_completion(wc, 2, client_ip);

      char* temp = static_cast<char*>(local_mr->addr);
      size_t namenumber_net;
      memcpy(&namenumber_net, temp, sizeof(size_t));
      size_t namenumber = htonl(namenumber_net);
      temp = temp + sizeof(size_t);

      char dbname_[namenumber+1];
      memcpy(dbname_, temp, namenumber);
      dbname_[namenumber] = '\0';
      temp = temp + namenumber;
      std::string db_name = std::string(dbname_);
//      std::cout << "save db_name is: " << db_name <<std::endl;
      if (fs_image.find(db_name)!= fs_image.end()){
        void* to_delete = fs_image.at(db_name)->addr;
        ibv_dereg_mr(fs_image.at(db_name));
        free(to_delete);
        fs_image.at(db_name) = local_mr;
      }else{
        fs_image[db_name] = local_mr;
      }





//      break;
    }else if(receive_msg_buf.command == save_log_serialized_data){
      printf("retrieve_log_serialized_data message received successfully\n");

      int buff_size = receive_msg_buf.content.fs_sync_cmd.data_size;
      file_type filetype = receive_msg_buf.content.fs_sync_cmd.type;
      post_receive(recv_mr,client_ip, 1000);

      post_send<int>(send_mr,client_ip);
      poll_completion(wc, 2, client_ip);

      std::string dbname;
      // Here could be some problem.
      dbname = std::string(recv_buff);
//      std::cout << "retrieve db_name is: " << dbname <<std::endl;
      ibv_mr* local_mr;
      if (log_image.find(dbname) == log_image.end()){
        void* buff = malloc(1024*1024);
        int mr_flags =
            IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
        local_mr = ibv_reg_mr(res->pd, static_cast<void*>(buff), buff_size, mr_flags);
        log_image.insert({dbname, local_mr});
      }else{
        local_mr = log_image.at(dbname);
      }
      post_receive(local_mr, client_ip, buff_size);
      post_receive<computing_to_memory_msg>(recv_mr, client_ip);
      post_send<int>(send_mr,client_ip);
      poll_completion(wc, 2, client_ip);


    }else if(receive_msg_buf.command == retrieve_log_serialized_data){
      printf("retrieve_log_serialized_data message received successfully\n");
//      post_receive(recv_mr,client_ip, 1000);
//      post_send<int>(send_mr,client_ip);
//      // prepare the receive for db name, the name should not exceed 1000byte
//
//      poll_completion(wc, 2, client_ip);
//      std::string dbname;
//      // Here could be some problem.
//      dbname = std::string(recv_buff);
//      std::cout << "retrieve db_name is: " << dbname <<std::endl;
//      ibv_mr* local_mr;
//      std::shared_lock<std::shared_mutex> l(fs_image_mutex);
//      if (log_image.find(dbname)!= fs_image.end()){
//        local_mr = fs_image.at(dbname);
//        l.unlock();
//        *(reinterpret_cast<size_t*>(send_buff)) = local_mr->length;
//        post_send<size_t>(send_mr,client_ip);
//        post_receive<char>(recv_mr,client_ip);
//        post_send(local_mr,client_ip, local_mr->length);
//        poll_completion(wc, 3, client_ip);
//      }else{
//        l.unlock();
//        *(reinterpret_cast<size_t*>(send_buff)) = 0;
//        post_receive<computing_to_memory_msg>(recv_mr, client_ip);
//        post_send<size_t>(send_mr,client_ip);
//        poll_completion(wc, 1, client_ip);
//      }
    }else {
      printf("corrupt message from client.\n");

    }


  }
  return;
  // TODO: Build up a exit method for shared memory side, don't forget to destroy all the RDMA resourses.
}
void RDMA_Manager::Server_to_Client_Communication() {
  if (resources_create()) {
    fprintf(stderr, "failed to create resources\n");
  }
  int rc;


  if (rdma_config.gid_idx >= 0) {
    printf("checkpoint0");
    rc = ibv_query_gid(res->ib_ctx, rdma_config.ib_port, rdma_config.gid_idx,
                       &(res->my_gid));
    if (rc) {
      fprintf(stderr, "could not get gid for port %d, index %d\n",
              rdma_config.ib_port, rdma_config.gid_idx);
      return;
    }
  } else
    memset(&(res->my_gid), 0, sizeof res->my_gid);
  server_sock_connect(rdma_config.server_name, rdma_config.tcp_port);
}

//    Register the memory through ibv_reg_mr on the local side. this function will be called by both of the server side and client side.
bool RDMA_Manager::Local_Memory_Register(char** p2buffpointer,
                                         ibv_mr** p2mrpointer, size_t size,
                                         std::string pool_name) {
  int mr_flags = 0;
  if (pre_allocated_pool.empty()){

    *p2buffpointer = new char[size];
    if (!*p2buffpointer) {
      fprintf(stderr, "failed to malloc bytes to memory buffer\n");
      return false;
    }
    memset(*p2buffpointer, 0, size);

    /* register the memory buffer */
    mr_flags =
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
    //  auto start = std::chrono::high_resolution_clock::now();
    *p2mrpointer = ibv_reg_mr(res->pd, *p2buffpointer, size, mr_flags);
    //  auto stop = std::chrono::high_resolution_clock::now();
    //  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start); std::printf("Memory registeration size: %zu time elapse (%ld) us\n", size, duration.count());
    local_mem_pool.push_back(*p2mrpointer);
    fprintf(stdout,
            "New MR was registered with addr=%p, lkey=0x%x, rkey=0x%x, flags=0x%x, size=%lu, total registered size is %lu\n",
            (*p2mrpointer)->addr, (*p2mrpointer)->lkey, (*p2mrpointer)->rkey,
            mr_flags, size, total_registered_size);
  }else{
    *p2mrpointer = pre_allocated_pool.back();
    pre_allocated_pool.pop_back();
    *p2buffpointer = (char*)(*p2mrpointer)->addr;
  }

  if (!*p2mrpointer) {
    fprintf(
        stderr,
        "ibv_reg_mr failed with mr_flags=0x%x, size = %zu, region num = %zu\n",
        mr_flags, size, local_mem_pool.size());
    return false;
  } else if(pool_name!= "") {
    // if pool name == "", then no bit map will be created. The registered memory is used for remote compute node RDMA read and write
    // If chunk size equals 0, which means that this buffer should not be add to Local Bit Map, will not be regulated by the RDMA manager.

    int placeholder_num =
        (*p2mrpointer)->length /
        (name_to_size.at(
            pool_name));  // here we supposing the SSTables are 4 megabytes
    In_Use_Array* in_use_array = new In_Use_Array(placeholder_num, name_to_size.at(pool_name),
                              *p2mrpointer);
    // TODO: Modify it to allocate the memory according to the memory chunk types

    name_to_mem_pool.at(pool_name).insert({(*p2mrpointer)->addr, in_use_array});
  }
  else
    printf("Register memory for computing node\n");
  total_registered_size = total_registered_size + (*p2mrpointer)->length;


  return true;

};
bool RDMA_Manager::Preregister_Memory(int gb_number) {
  int mr_flags = 0;
  size_t size = 1024*1024*1024;

  for (int i = 0; i < gb_number; ++i) {
    //    total_registered_size = total_registered_size + size;
    std::fprintf(stderr, "Pre allocate registered memory %d GB %30s\r", i, "");
    std::fflush(stderr);
    char* buff_pointer = new char[size];
    if (!buff_pointer) {
      fprintf(stderr, "failed to malloc bytes to memory buffer\n");
      return false;
    }
    memset(buff_pointer, 0, size);

    /* register the memory buffer */
    mr_flags =
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
    //  auto start = std::chrono::high_resolution_clock::now();
    ibv_mr* mrpointer = ibv_reg_mr(res->pd, buff_pointer, size, mr_flags);
    if (!mrpointer) {
      fprintf(
          stderr,
          "ibv_reg_mr failed with mr_flags=0x%x, size = %zu, region num = %zu\n",
          mr_flags, size, local_mem_pool.size());
      return false;
    }
    local_mem_pool.push_back(mrpointer);
    pre_allocated_pool.push_back(mrpointer);
  }
  return true;
}
/******************************************************************************
* Function: set_up_RDMA
*
* Input
* argv0 command line arguments
*
* Output
* none
*
* Returns
* none
*
* Description
* set up the connection to shared memroy.
******************************************************************************/
void RDMA_Manager::Client_Set_Up_Resources() {
  //  int rc = 1;
  // int trans_times;
  char temp_char;
  std::string ip_add;
  printf("please input the ipadress for shared memory");
  std::cin >> ip_add;
  rdma_config.server_name = ip_add.c_str();
  /* if client side */

  res->sock_map["main"] =
      client_sock_connect(rdma_config.server_name, rdma_config.tcp_port);
  if (res->sock_map["main"] < 0) {
    fprintf(stderr,
            "failed to establish TCP connection to server %s, port %d\n",
            rdma_config.server_name, rdma_config.tcp_port);
  }

  if (resources_create()) {
    fprintf(stderr, "failed to create resources\n");
    return;
  }
  void* buff = malloc(1024*1024);
  int mr_flags =
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
  log_image_mr = ibv_reg_mr(res->pd, buff, 1024*1024, mr_flags);
  Client_Connect_to_Server_RDMA();
}
/******************************************************************************
* Function: resources_create
*
* Input
* res pointer to resources structure to be filled in
*
* Output
* res filled in with resources
*
* Returns
* 0 on success, 1 on failure
*
* Description
*
* This function creates and allocates all necessary system resources. These
* are stored in res.
*****************************************************************************/
int RDMA_Manager::resources_create() {
  struct ibv_device** dev_list = NULL;
  struct ibv_device* ib_dev = NULL;
//  int iter = 1;
  int i;

//  int cq_size = 0;
  int num_devices;
  int rc = 0;
  //        ibv_device_attr *device_attr;

  fprintf(stdout, "searching for IB devices in host\n");
  /* get device names in the system */
  dev_list = ibv_get_device_list(&num_devices);
  if (!dev_list) {
    fprintf(stderr, "failed to get IB devices list\n");
    rc = 1;
  }
  /* if there isn't any IB device in host */
  if (!num_devices) {
    fprintf(stderr, "found %d device(s)\n", num_devices);
    rc = 1;
  }
  fprintf(stdout, "found %d device(s)\n", num_devices);
  /* search for the specific device we want to work with */
  for (i = 0; i < num_devices; i++) {
    if (!rdma_config.dev_name) {
      rdma_config.dev_name = strdup(ibv_get_device_name(dev_list[i]));
      fprintf(stdout, "device not specified, using first one found: %s\n",
              rdma_config.dev_name);
    }
    if (!strcmp(ibv_get_device_name(dev_list[i]), rdma_config.dev_name)) {
      ib_dev = dev_list[i];
      break;
    }
  }
  /* if the device wasn't found in host */
  if (!ib_dev) {
    fprintf(stderr, "IB device %s wasn't found\n", rdma_config.dev_name);
    rc = 1;
  }
  /* get device handle */
  res->ib_ctx = ibv_open_device(ib_dev);
  if (!res->ib_ctx) {
    fprintf(stderr, "failed to open device %s\n", rdma_config.dev_name);
    rc = 1;
  }
  /* We are now done with device list, free it */
  ibv_free_device_list(dev_list);
  dev_list = NULL;
  ib_dev = NULL;
  /* query port properties */
  if (ibv_query_port(res->ib_ctx, rdma_config.ib_port, &res->port_attr)) {
    fprintf(stderr, "ibv_query_port on port %u failed\n", rdma_config.ib_port);
    rc = 1;
  }
  /* allocate Protection Domain */
  res->pd = ibv_alloc_pd(res->ib_ctx);
  if (!res->pd) {
    fprintf(stderr, "ibv_alloc_pd failed\n");
    rc = 1;
  }

  /* computing node allocate local buffers */
//  if (rdma_config.server_name) {
//    ibv_mr* mr;
//    char* buff;
//    if (!Local_Memory_Register(&buff, &mr, rdma_config.init_local_buffer_size,
//                               0)) {
//      fprintf(stderr, "memory registering failed by size of 0x%x\n",
//              static_cast<unsigned>(rdma_config.init_local_buffer_size));
//    } else {
//      fprintf(stdout, "memory registering succeed by size of 0x%x\n",
//              static_cast<unsigned>(rdma_config.init_local_buffer_size));
//    }
//  }
  Local_Memory_Register(&(res->send_buf), &(res->mr_send), 1000, std::string());
  Local_Memory_Register(&(res->receive_buf), &(res->mr_receive), 1000,
                        std::string());
  //        if(condition){
  //          fprintf(stderr, "Local memory registering failed\n");
  //
  //        }

  fprintf(stdout, "SST buffer, send&receive buffer were registered with a\n");
  rc = ibv_query_device(res->ib_ctx, &(res->device_attr));
  std::cout << "maximum query pair number is" << res->device_attr.max_qp
            << std::endl;
  std::cout << "maximum completion queue number is" << res->device_attr.max_cq
            << std::endl;
  std::cout << "maximum memory region number is" << res->device_attr.max_mr
            << std::endl;
  std::cout << "maximum memory region size is" << res->device_attr.max_mr_size
            << std::endl;

  return rc;
}

bool RDMA_Manager::Client_Connect_to_Server_RDMA() {
  //  int iter = 1;
  char temp_receive[2];
  char temp_send[] = "Q";
  struct registered_qp_config local_con_data;
  struct registered_qp_config remote_con_data;
  struct registered_qp_config tmp_con_data;
  std::string qp_id = "main";
  int rc = 0;

  union ibv_gid my_gid;
  if (rdma_config.gid_idx >= 0) {
    rc = ibv_query_gid(res->ib_ctx, rdma_config.ib_port, rdma_config.gid_idx,
                       &my_gid);
    if (rc) {
      fprintf(stderr, "could not get gid for port %d, index %d\n",
              rdma_config.ib_port, rdma_config.gid_idx);
      return rc;
    }
  } else
    memset(&my_gid, 0, sizeof my_gid);
  /* exchange using TCP sockets info required to connect QPs */
  create_qp(qp_id);
  local_con_data.qp_num = htonl(res->qp_map[qp_id]->qp_num);
  local_con_data.lid = htons(res->port_attr.lid);
  memcpy(local_con_data.gid, &my_gid, 16);
  fprintf(stdout, "\nLocal LID = 0x%x\n", res->port_attr.lid);
  if (sock_sync_data(res->sock_map["main"], sizeof(struct registered_qp_config),
                     (char*)&local_con_data, (char*)&tmp_con_data) < 0) {
    fprintf(stderr, "failed to exchange connection data between sides\n");
    rc = 1;
  }
  remote_con_data.qp_num = ntohl(tmp_con_data.qp_num);
  remote_con_data.lid = ntohs(tmp_con_data.lid);
  memcpy(remote_con_data.gid, tmp_con_data.gid, 16);

  fprintf(stdout, "Remote QP number = 0x%x\n", remote_con_data.qp_num);
  fprintf(stdout, "Remote LID = 0x%x\n", remote_con_data.lid);
  connect_qp(remote_con_data, qp_id);
//  post_receive<int>(res->mr_receive, std::string("main"));
  if (sock_sync_data(res->sock_map["main"], 1, temp_send,
                     temp_receive)) /* just send a dummy char back and forth */
  {
    fprintf(stderr, "sync error after QPs are were moved to RTS\n");
    rc = 1;
  }

  // sync the communication by rdma.

//  post_send<int>(res->mr_send, std::string("main"));
//  ibv_wc wc[2] = {};
//  if(!poll_completion(wc, 2, std::string("main"))){
//    return true;
//  }else{
//    printf("The main qp not create correctly");
//    return false;
//  }

  return false;
}
bool RDMA_Manager::create_qp(std::string& id) {
  struct ibv_qp_init_attr qp_init_attr;

  /* each side will send only one WR, so Completion Queue with 1 entry is enough
   */
  int cq_size = 2500;
  ibv_cq* cq = ibv_create_cq(res->ib_ctx, cq_size, NULL, NULL, 0);
  if (!cq) {
    fprintf(stderr, "failed to create CQ with %u entries\n", cq_size);
  }
  std::unique_lock<std::shared_mutex> l(qp_cq_map_mutex);
  if (id != "")
    res->cq_map[id] = cq;
  else
    cq_local->Reset(cq);

  /* create the Queue Pair */
  memset(&qp_init_attr, 0, sizeof(qp_init_attr));
  qp_init_attr.qp_type = IBV_QPT_RC;
  qp_init_attr.sq_sig_all = 0;
  qp_init_attr.send_cq = cq;
  qp_init_attr.recv_cq = cq;
  qp_init_attr.cap.max_send_wr = 2500;
  qp_init_attr.cap.max_recv_wr = 2500;
  qp_init_attr.cap.max_send_sge = 30;
  qp_init_attr.cap.max_recv_sge = 30;
//  qp_init_attr.cap.max_inline_data = -1;
  ibv_qp* qp = ibv_create_qp(res->pd, &qp_init_attr);
  if (!qp) {
    fprintf(stderr, "failed to create QP, errno: %d\n", errno);
    exit(1);
  }
  if (id != "")
    res->qp_map[id] = qp;
  else
    qp_local->Reset(qp);
#ifndef NDEBUG
  fprintf(stdout, "QP was created, QP number=0x%x\n", qp->qp_num);
#endif
  return true;
}
/******************************************************************************
* Function: connect_qp
*
* Input
* res pointer to resources structure
*
* Output
* none
*
* Returns
* 0 on success, error code on failure
*
* Description
* Connect the QP. Transition the server side to RTR, sender side to RTS
******************************************************************************/
int RDMA_Manager::connect_qp(registered_qp_config remote_con_data,
                             std::string& qp_id) {
  int rc;
  ibv_qp* qp;
  if (qp_id != "")
    qp = res->qp_map[qp_id];
  else
    qp = static_cast<ibv_qp*>(qp_local->Get());
  if (rdma_config.gid_idx >= 0) {
    uint8_t* p = remote_con_data.gid;
#ifndef NDEBUG
    fprintf(stdout,
            "Remote GID =%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x\n ",
            p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10],
            p[11], p[12], p[13], p[14], p[15]);
#endif
  }
  /* modify the QP to init */
  rc = modify_qp_to_init(qp);
  if (rc) {
    fprintf(stderr, "change QP state to INIT failed\n");
    goto connect_qp_exit;
  }

  /* modify the QP to RTR */
  rc = modify_qp_to_rtr(qp, remote_con_data.qp_num,
                        remote_con_data.lid, remote_con_data.gid);
  if (rc) {
    fprintf(stderr, "failed to modify QP state to RTR\n");
    goto connect_qp_exit;
  }
  rc = modify_qp_to_rts(qp);
  if (rc) {
#ifndef NDEBUG
    fprintf(stderr, "failed to modify QP state to RTS\n");
#endif
    goto connect_qp_exit;
  }
#ifndef NDEBUG
  fprintf(stdout, "QP %s state was change to RTS\n", qp_id.c_str());
#endif
  /* sync to make sure that both sides are in states that they can connect to prevent packet loose */
  connect_qp_exit:
  return rc;
}
/******************************************************************************
* Function: modify_qp_to_init
*
* Input
* qp QP to transition
*
* Output
* none
*
* Returns
* 0 on success, ibv_modify_qp failure code on failure
*
* Description
* Transition a QP from the RESET to INIT state
******************************************************************************/
int RDMA_Manager::modify_qp_to_init(struct ibv_qp* qp) {
  struct ibv_qp_attr attr;
  int flags;
  int rc;
  memset(&attr, 0, sizeof(attr));
  attr.qp_state = IBV_QPS_INIT;
  attr.port_num = rdma_config.ib_port;
  attr.pkey_index = 0;
  attr.qp_access_flags =
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
  flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
  rc = ibv_modify_qp(qp, &attr, flags);
  if (rc) fprintf(stderr, "failed to modify QP state to INIT\n");
  return rc;
}
/******************************************************************************
* Function: modify_qp_to_rtr
*
* Input
* qp QP to transition
* remote_qpn remote QP number
* dlid destination LID
* dgid destination GID (mandatory for RoCEE)
*
* Output
* none
*
* Returns
* 0 on success, ibv_modify_qp failure code on failure
*
* Description
* Transition a QP from the INIT to RTR state, using the specified QP number
******************************************************************************/
int RDMA_Manager::modify_qp_to_rtr(struct ibv_qp* qp, uint32_t remote_qpn,
                                   uint16_t dlid, uint8_t* dgid) {
  struct ibv_qp_attr attr;
  int flags;
  int rc;
  memset(&attr, 0, sizeof(attr));
  attr.qp_state = IBV_QPS_RTR;
  attr.path_mtu = IBV_MTU_4096;
  attr.dest_qp_num = remote_qpn;
  attr.rq_psn = 0;
  attr.max_dest_rd_atomic = 1;
  attr.min_rnr_timer = 0xc;
  attr.ah_attr.is_global = 0;
  attr.ah_attr.dlid = dlid;
  attr.ah_attr.sl = 0;
  attr.ah_attr.src_path_bits = 0;
  attr.ah_attr.port_num = rdma_config.ib_port;
  if (rdma_config.gid_idx >= 0) {
    attr.ah_attr.is_global = 1;
    attr.ah_attr.port_num = 1;
    memcpy(&attr.ah_attr.grh.dgid, dgid, 16);
    attr.ah_attr.grh.flow_label = 0;
    attr.ah_attr.grh.hop_limit = 0xFF;
    attr.ah_attr.grh.sgid_index = rdma_config.gid_idx;
    attr.ah_attr.grh.traffic_class = 0;
  }
  flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
          IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
  rc = ibv_modify_qp(qp, &attr, flags);
  if (rc) fprintf(stderr, "failed to modify QP state to RTR\n");
  return rc;
}
/******************************************************************************
* Function: modify_qp_to_rts
*
* Input
* qp QP to transition
*
* Output
* none
*
* Returns
* 0 on success, ibv_modify_qp failure code on failure
*
* Description
* Transition a QP from the RTR to RTS state
******************************************************************************/
int RDMA_Manager::modify_qp_to_rts(struct ibv_qp* qp) {
  struct ibv_qp_attr attr;
  int flags;
  int rc;
  memset(&attr, 0, sizeof(attr));
  attr.qp_state = IBV_QPS_RTS;
  attr.timeout = 0xe;
  attr.retry_cnt = 7;
  attr.rnr_retry = 7;
  attr.sq_psn = 0;
  attr.max_rd_atomic = 1;
  flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY |
          IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;
  rc = ibv_modify_qp(qp, &attr, flags);
  if (rc) fprintf(stderr, "failed to modify QP state to RTS\n");
  return rc;
}
/******************************************************************************
* Function: sock_sync_data
*
* Input
* sock socket to transfer data on
* xfer_size size of data to transfer
* local_data pointer to data to be sent to remote
*
* Output
* remote_data pointer to buffer to receive remote data
*
* Returns
* 0 on success, negative error code on failure
*
* Description
* Sync data across a socket. The indicated local data will be sent to the
* remote. It will then wait for the remote to send its data back. It is
* assumed that the two sides are in sync and call this function in the proper
* order. Chaos will ensue if they are not. :)
*
* Also note this is a blocking function and will wait for the full data to be
* received from the remote.
*
******************************************************************************/
int RDMA_Manager::sock_sync_data(int sock, int xfer_size, char* local_data,
                                 char* remote_data) {
  int rc;
  int read_bytes = 0;
  int total_read_bytes = 0;
  rc = write(sock, local_data, xfer_size);
  if (rc < xfer_size)
    fprintf(stderr,
            "Failed writing data during sock_sync_data, total bytes are %d\n",
            rc);
  else
    rc = 0;
  printf("total bytes: %d", xfer_size);
  while (!rc && total_read_bytes < xfer_size) {
    read_bytes = read(sock, remote_data, xfer_size);
    printf("read byte: %d", read_bytes);
    if (read_bytes > 0)
      total_read_bytes += read_bytes;
    else
      rc = read_bytes;
  }
  fprintf(stdout, "The data which has been read through is %s size is %d\n",
          remote_data, read_bytes);
  return rc;
}
/******************************************************************************
End of socket operations
******************************************************************************/

// return 0 means success
int
RDMA_Manager::RDMA_Read(ibv_mr *remote_mr, ibv_mr *local_mr, size_t msg_size, std::string q_id, size_t send_flag,
                        int poll_num) {
//#ifdef GETANALYSIS
//  auto start = std::chrono::high_resolution_clock::now();
//#endif
  struct ibv_send_wr sr;
  struct ibv_sge sge;
  struct ibv_send_wr* bad_wr = NULL;
  int rc;
  /* prepare the scatter/gather entry */
  memset(&sge, 0, sizeof(sge));
  sge.addr = (uintptr_t)local_mr->addr;
  sge.length = msg_size;
  sge.lkey = local_mr->lkey;
  /* prepare the send work request */
  memset(&sr, 0, sizeof(sr));
  sr.next = NULL;
  sr.wr_id = 0;
  sr.sg_list = &sge;
  sr.num_sge = 1;
  sr.opcode = IBV_WR_RDMA_READ;
  if (send_flag != 0 )
    sr.send_flags = send_flag;
//  printf("send flag to transform is %u", send_flag);
//  printf("send flag is %u", sr.send_flags);
  sr.wr.rdma.remote_addr = reinterpret_cast<uint64_t>(remote_mr->addr);
  sr.wr.rdma.rkey = remote_mr->rkey;

  /* there is a Receive Request in the responder side, so we won't get any into RNR flow */
  //*(start) = std::chrono::steady_clock::now();
  // start = std::chrono::steady_clock::now();
//  auto stop = std::chrono::high_resolution_clock::now();
//  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//  std::printf("rdma read  send prepare for (%zu), time elapse : (%ld)\n", msg_size, duration.count());
//  start = std::chrono::high_resolution_clock::now();
  if (q_id != ""){
    std::shared_lock<std::shared_mutex> l(qp_cq_map_mutex);
    rc = ibv_post_send(res->qp_map.at(q_id), &sr, &bad_wr);
    l.unlock();
  }
  else{
    ibv_qp* qp = static_cast<ibv_qp*>(qp_local->Get());
    if (qp == NULL){
      Remote_Query_Pair_Connection(q_id);
      qp = static_cast<ibv_qp*>(qp_local->Get());
    }
    std::shared_lock<std::shared_mutex> l(qp_cq_map_mutex);
    rc = ibv_post_send(qp, &sr, &bad_wr);
    l.unlock();
  }
//    std::cout << " " << msg_size << "time elapse :" <<  << std::endl;
//  start = std::chrono::high_resolution_clock::now();

  if (rc) {
    fprintf(stderr, "failed to post SR %s \n", q_id.c_str());
    exit(1);

  }else{
//      printf("qid: %s", q_id.c_str());
  }
  //  else
  //  {
  //    fprintf(stdout, "RDMA Read Request was posted, OPCODE is %d\n", sr.opcode);
  //  }
  if (poll_num != 0){
    ibv_wc* wc = new ibv_wc[poll_num]();
    //  auto start = std::chrono::high_resolution_clock::now();
    //  while(std::chrono::high_resolution_clock::now
    //  ()-start < std::chrono::nanoseconds(msg_size+200000));
    rc = poll_completion(wc, poll_num, q_id);
    if (rc != 0) {
      std::cout << "RDMA Read Failed" << std::endl;
      std::cout << "q id is" << q_id << std::endl;
      fprintf(stdout, "QP number=0x%x\n", res->qp_map[q_id]->qp_num);
    }
    delete[] wc;
  }
//#ifdef GETANALYSIS
//  auto stop = std::chrono::high_resolution_clock::now();
//  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
////    std::printf("Get from SSTables (not found) time elapse is %zu\n",  duration.count());
//  if (msg_size <= 8192){
//    RDMA_Manager::RDMAReadTimeElapseSum.fetch_add(duration.count());
//    RDMA_Manager::ReadCount.fetch_add(1);
//  }
//
//#endif
  return rc;
}
int RDMA_Manager::RDMA_Write(ibv_mr* remote_mr, ibv_mr* local_mr,
                             size_t msg_size, std::string q_id, size_t send_flag,
                             int poll_num) {
//  auto start = std::chrono::high_resolution_clock::now();
  struct ibv_send_wr sr;
  struct ibv_sge sge;
  struct ibv_send_wr* bad_wr = NULL;
  int rc;
  /* prepare the scatter/gather entry */
  memset(&sge, 0, sizeof(sge));
  sge.addr = (uintptr_t)local_mr->addr;
  sge.length = msg_size;
  sge.lkey = local_mr->lkey;
  /* prepare the send work request */
  memset(&sr, 0, sizeof(sr));
  sr.next = NULL;
  sr.wr_id = 0;
  sr.sg_list = &sge;
  sr.num_sge = 1;
  sr.opcode = IBV_WR_RDMA_WRITE;
  if (send_flag != 0 )
    sr.send_flags = send_flag;
  sr.wr.rdma.remote_addr = reinterpret_cast<uint64_t>(remote_mr->addr);
  sr.wr.rdma.rkey = remote_mr->rkey;
  /* there is a Receive Request in the responder side, so we won't get any into RNR flow */
  //*(start) = std::chrono::steady_clock::now();
  // start = std::chrono::steady_clock::now();
//  auto stop = std::chrono::high_resolution_clock::now();
//  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//  printf("RDMA Write send preparation size: %zu elapse: %ld\n", msg_size, duration.count());
//  start = std::chrono::high_resolution_clock::now();

  if (q_id != ""){
    std::shared_lock<std::shared_mutex> l(qp_cq_map_mutex);
    rc = ibv_post_send(res->qp_map.at(q_id), &sr, &bad_wr);
    l.unlock();
  }
  else{
    ibv_qp* qp = static_cast<ibv_qp*>(qp_local->Get());
    if (qp == NULL){
      Remote_Query_Pair_Connection(q_id);
      qp = static_cast<ibv_qp*>(qp_local->Get());
    }
    std::shared_lock<std::shared_mutex> l(qp_cq_map_mutex);
    rc = ibv_post_send(qp, &sr, &bad_wr);
    l.unlock();
  }

  //  start = std::chrono::high_resolution_clock::now();
  if (rc) fprintf(stderr, "failed to post SR\n");
  //  else
  //  {
  //    fprintf(stdout, "RDMA Write Request was posted, OPCODE is %d\n", sr.opcode);
  //  }
  if (poll_num != 0){
    ibv_wc* wc = new ibv_wc[poll_num]();
    //  auto start = std::chrono::high_resolution_clock::now();
    //  while(std::chrono::high_resolution_clock::now()-start < std::chrono::nanoseconds(msg_size+200000));
    // wait until the job complete.
    rc = poll_completion(wc, poll_num, q_id);
    if (rc != 0) {
      std::cout << "RDMA Write Failed" << std::endl;
      std::cout << "q id is" << q_id << std::endl;
      fprintf(stdout, "QP number=0x%x\n", res->qp_map[q_id]->qp_num);
    }
    delete []  wc;
  }
//  stop = std::chrono::high_resolution_clock::now();
//  duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//  printf("RDMA Write post send and poll size: %zu elapse: %ld\n", msg_size, duration.count());
  return rc;
}
// int RDMA_Manager::post_atomic(int opcode)
//{
//  struct ibv_send_wr sr;
//  struct ibv_sge sge;
//  struct ibv_send_wr* bad_wr = NULL;
//  int rc;
//  extern int msg_size;
//  /* prepare the scatter/gather entry */
//  memset(&sge, 0, sizeof(sge));
//  sge.addr = (uintptr_t)res->send_buf;
//  sge.length = msg_size;
//  sge.lkey = res->mr_receive->lkey;
//  /* prepare the send work request */
//  memset(&sr, 0, sizeof(sr));
//  sr.next = NULL;
//  sr.wr_id = 0;
//  sr.sg_list = &sge;
//  sr.num_sge = 1;
//  sr.opcode = static_cast<ibv_wr_opcode>(IBV_WR_SEND);
//  sr.send_flags = IBV_SEND_SIGNALED;
//  if (opcode != IBV_WR_SEND)
//  {
//    sr.wr.rdma.remote_addr = res->mem_regions.addr;
//    sr.wr.rdma.rkey = res->mem_regions.rkey;
//  }
//  /* there is a Receive Request in the responder side, so we won't get any into RNR flow */
//  //*(start) = std::chrono::steady_clock::now();
//  //start = std::chrono::steady_clock::now();
//  rc = ibv_post_send(res->qp, &sr, &bad_wr);
//  if (rc)
//    fprintf(stderr, "failed to post SR\n");
//  else
//  {
//    /*switch (opcode)
//    {
//    case IBV_WR_SEND:
//            fprintf(stdout, "Send Request was posted\n");
//            break;
//    case IBV_WR_RDMA_READ:
//            fprintf(stdout, "RDMA Read Request was posted\n");
//            break;
//    case IBV_WR_RDMA_WRITE:
//            fprintf(stdout, "RDMA Write Request was posted\n");
//            break;
//    default:
//            fprintf(stdout, "Unknown Request was posted\n");
//            break;
//    }*/
//  }
//  return rc;
//}
/******************************************************************************
* Function: post_send
*
* Input
* res pointer to resources structure
* opcode IBV_WR_SEND, IBV_WR_RDMA_READ or IBV_WR_RDMA_WRITE
*
* Output
* none
*
* Returns
* 0 on success, error code on failure
*
* Description
* This function will create and post a send work request
******************************************************************************/
template <typename T>
int RDMA_Manager::post_send(ibv_mr* mr, std::string qp_id) {
  struct ibv_send_wr sr;
  struct ibv_sge sge;
  struct ibv_send_wr* bad_wr = NULL;
  int rc;
//  if (!rdma_config.server_name) {
  // server side.
  /* prepare the scatter/gather entry */
  memset(&sge, 0, sizeof(sge));
  sge.addr = (uintptr_t)mr->addr;
  sge.length = sizeof(T);
  sge.lkey = mr->lkey;
//  }
//  else {
//    //client side
//    /* prepare the scatter/gather entry */
//    memset(&sge, 0, sizeof(sge));
//    sge.addr = (uintptr_t)res->send_buf;
//    sge.length = sizeof(T);
//    sge.lkey = res->mr_send->lkey;
//  }

  /* prepare the send work request */
  memset(&sr, 0, sizeof(sr));
  sr.next = NULL;
  sr.wr_id = 0;
  sr.sg_list = &sge;
  sr.num_sge = 1;
  sr.opcode = static_cast<ibv_wr_opcode>(IBV_WR_SEND);
  sr.send_flags = IBV_SEND_SIGNALED;

  /* there is a Receive Request in the responder side, so we won't get any into RNR flow */
  //*(start) = std::chrono::steady_clock::now();
  // start = std::chrono::steady_clock::now();

  if (rdma_config.server_name)
    rc = ibv_post_send(res->qp_map["main"], &sr, &bad_wr);
  else
    rc = ibv_post_send(res->qp_map[qp_id], &sr, &bad_wr);
  if (rc)
    fprintf(stderr, "failed to post SR\n");
//  else {
//    fprintf(stdout, "Send Request was posted\n");
//  }
  return rc;
}
int RDMA_Manager::post_send(ibv_mr* mr, std::string qp_id, size_t size) {
  struct ibv_send_wr sr;
  struct ibv_sge sge;
  struct ibv_send_wr* bad_wr = NULL;
  int rc;
//  if (!rdma_config.server_name) {
  /* prepare the scatter/gather entry */
  memset(&sge, 0, sizeof(sge));
  sge.addr = (uintptr_t)mr->addr;
  sge.length = size;
  sge.lkey = mr->lkey;
//  }
//  else {
//    /* prepare the scatter/gather entry */
//    memset(&sge, 0, sizeof(sge));
//    sge.addr = (uintptr_t)res->send_buf;
//    sge.length = size;
//    sge.lkey = res->mr_send->lkey;
//  }

  /* prepare the send work request */
  memset(&sr, 0, sizeof(sr));
  sr.next = NULL;
  sr.wr_id = 0;
  sr.sg_list = &sge;
  sr.num_sge = 1;
  sr.opcode = static_cast<ibv_wr_opcode>(IBV_WR_SEND);
  sr.send_flags = IBV_SEND_SIGNALED;

  /* there is a Receive Request in the responder side, so we won't get any into RNR flow */
  //*(start) = std::chrono::steady_clock::now();
  // start = std::chrono::steady_clock::now();

  if (rdma_config.server_name)
    rc = ibv_post_send(res->qp_map["main"], &sr, &bad_wr);
  else
    rc = ibv_post_send(res->qp_map[qp_id], &sr, &bad_wr);
  if (rc)
    fprintf(stderr, "failed to post SR\n");
//  else {
//    fprintf(stdout, "Send Request was posted\n");
//  }
  return rc;
}
int RDMA_Manager::post_send(ibv_mr** mr_list, size_t sge_size,
                            std::string qp_id) {
  struct ibv_send_wr sr;
  struct ibv_sge sge[sge_size];
  struct ibv_send_wr* bad_wr = NULL;
  int rc;
//  if (!rdma_config.server_name) {
  /* prepare the scatter/gather entry */
  for(size_t i = 0; i < sge_size; i++){
    memset(&sge[i], 0, sizeof(sge));
    sge[i].addr = (uintptr_t)mr_list[i]->addr;
    sge[i].length = mr_list[i]->length;
    sge[i].lkey = mr_list[i]->lkey;
  }

//  }
//  else {
//    /* prepare the scatter/gather entry */
//    memset(&sge, 0, sizeof(sge));
//    sge.addr = (uintptr_t)res->send_buf;
//    sge.length = size;
//    sge.lkey = res->mr_send->lkey;
//  }

  /* prepare the send work request */
  memset(&sr, 0, sizeof(sr));
  sr.next = NULL;
  sr.wr_id = 0;
  sr.sg_list = sge;
  sr.num_sge = sge_size;
  sr.opcode = static_cast<ibv_wr_opcode>(IBV_WR_SEND);
  sr.send_flags = IBV_SEND_SIGNALED;

  /* there is a Receive Request in the responder side, so we won't get any into RNR flow */
  //*(start) = std::chrono::steady_clock::now();
  // start = std::chrono::steady_clock::now();

  if (rdma_config.server_name)
    rc = ibv_post_send(res->qp_map["main"], &sr, &bad_wr);
  else
    rc = ibv_post_send(res->qp_map[qp_id], &sr, &bad_wr);
  if (rc)
    fprintf(stderr, "failed to post SR\n");
//  else {
//    fprintf(stdout, "Send Request was posted\n");
//  }
  return rc;
}
int RDMA_Manager::post_receive(ibv_mr** mr_list, size_t sge_size,
                               std::string qp_id) {
  struct ibv_recv_wr rr;
  struct ibv_sge sge[sge_size];
  struct ibv_recv_wr* bad_wr;
  int rc;
//  if (!rdma_config.server_name) {
  /* prepare the scatter/gather entry */

  for(size_t i = 0; i < sge_size; i++){
    memset(&sge[i], 0, sizeof(sge));
    sge[i].addr = (uintptr_t)mr_list[i]->addr;
    sge[i].length = mr_list[i]->length;
    sge[i].lkey = mr_list[i]->lkey;
  }

//  }
//  else {
//    /* prepare the scatter/gather entry */
//    memset(&sge, 0, sizeof(sge));
//    sge.addr = (uintptr_t)res->receive_buf;
//    sge.length = size;
//    sge.lkey = res->mr_receive->lkey;
//  }

  /* prepare the receive work request */
  memset(&rr, 0, sizeof(rr));
  rr.next = NULL;
  rr.wr_id = 0;
  rr.sg_list = sge;
  rr.num_sge = sge_size;
  /* post the Receive Request to the RQ */
  if (rdma_config.server_name)
    rc = ibv_post_recv(res->qp_map["main"], &rr, &bad_wr);
  else
    rc = ibv_post_recv(res->qp_map[qp_id], &rr, &bad_wr);
  if (rc)
    fprintf(stderr, "failed to post RR\n");
//  else
//    fprintf(stdout, "Receive Request was posted\n");
  return rc;
}

int RDMA_Manager::post_receive(ibv_mr* mr, std::string qp_id, size_t size) {
  struct ibv_recv_wr rr;
  struct ibv_sge sge;
  struct ibv_recv_wr* bad_wr;
  int rc;
//  if (!rdma_config.server_name) {
  /* prepare the scatter/gather entry */

  memset(&sge, 0, sizeof(sge));
  sge.addr = (uintptr_t)mr->addr;
  sge.length = size;
  sge.lkey = mr->lkey;

//  }
//  else {
//    /* prepare the scatter/gather entry */
//    memset(&sge, 0, sizeof(sge));
//    sge.addr = (uintptr_t)res->receive_buf;
//    sge.length = size;
//    sge.lkey = res->mr_receive->lkey;
//  }

  /* prepare the receive work request */
  memset(&rr, 0, sizeof(rr));
  rr.next = NULL;
  rr.wr_id = 0;
  rr.sg_list = &sge;
  rr.num_sge = 1;
  /* post the Receive Request to the RQ */
  if (rdma_config.server_name)
    rc = ibv_post_recv(res->qp_map["main"], &rr, &bad_wr);
  else
    rc = ibv_post_recv(res->qp_map[qp_id], &rr, &bad_wr);
  if (rc)
    fprintf(stderr, "failed to post RR\n");
//  else
//    fprintf(stdout, "Receive Request was posted\n");
  return rc;
}

/******************************************************************************
* Function: post_receive
*
* Input
* res pointer to resources structure
*
* Output
* none
*
* Returns
* 0 on success, error code on failure
*
* Description
*
******************************************************************************/
// TODO: Add templete for post send and post receive, making the type of transfer data configurable.
template <typename T>
int RDMA_Manager::post_receive(ibv_mr* mr, std::string qp_id) {
  struct ibv_recv_wr rr;
  struct ibv_sge sge;
  struct ibv_recv_wr* bad_wr;
  int rc;
//  if (!rdma_config.server_name) {
//    /* prepare the scatter/gather entry */

  memset(&sge, 0, sizeof(sge));
  sge.addr = (uintptr_t)mr->addr;
  sge.length = sizeof(T);
  sge.lkey = mr->lkey;

//  }
//  else {
//    /* prepare the scatter/gather entry */
//    memset(&sge, 0, sizeof(sge));
//    sge.addr = (uintptr_t)res->receive_buf;
//    sge.length = sizeof(T);
//    sge.lkey = res->mr_receive->lkey;
//  }

  /* prepare the receive work request */
  memset(&rr, 0, sizeof(rr));
  rr.next = NULL;
  rr.wr_id = 0;
  rr.sg_list = &sge;
  rr.num_sge = 1;
  /* post the Receive Request to the RQ */
  if (rdma_config.server_name)
    rc = ibv_post_recv(res->qp_map["main"], &rr, &bad_wr);
  else
    rc = ibv_post_recv(res->qp_map[qp_id], &rr, &bad_wr);
  if (rc)
    fprintf(stderr, "failed to post RR\n");
//  else
//    fprintf(stdout, "Receive Request was posted\n");
  return rc;
}
/* poll_completion */
/******************************************************************************
* Function: poll_completion
*
* Input
* res pointer to resources structure
*
* Output
* none
*
* Returns
* 0 on success, 1 on failure
*
* Description
* Poll the completion queue for a single event. This function will continue to
* poll the queue until MAX_POLL_CQ_TIMEOUT milliseconds have passed.
*
******************************************************************************/
int RDMA_Manager::poll_completion(ibv_wc* wc_p, int num_entries,
                                  std::string q_id) {
  // unsigned long start_time_msec;
  // unsigned long cur_time_msec;
  // struct timeval cur_time;
  int poll_result;
  int poll_num = 0;
  int rc = 0;
  ibv_cq* cq;
  /* poll the completion for a while before giving up of doing it .. */
  // gettimeofday(&cur_time, NULL);
  // start_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);
  std::shared_lock<std::shared_mutex> l(qp_cq_map_mutex);
  if (q_id != "")
    cq =  res->cq_map.at(q_id);
  else
    cq =  static_cast<ibv_cq*>(cq_local->Get());
  l.unlock();
  do {

    poll_result = ibv_poll_cq(cq, num_entries, wc_p);
    if (poll_result < 0)
      break;
    else
      poll_num = poll_num + poll_result;
    /*gettimeofday(&cur_time, NULL);
    cur_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);*/
  } while (poll_num < num_entries);  // && ((cur_time_msec - start_time_msec) < MAX_POLL_CQ_TIMEOUT));
  //*(end) = std::chrono::steady_clock::now();
  // end = std::chrono::steady_clock::now();
  if (poll_result < 0) {
    /* poll CQ failed */
    fprintf(stderr, "poll CQ failed\n");
    rc = 1;
  } else if (poll_result == 0) { /* the CQ is empty */
    fprintf(stderr, "completion wasn't found in the CQ after timeout\n");
    rc = 1;
  } else {
    /* CQE found */
    // fprintf(stdout, "completion was found in CQ with status 0x%x\n", wc.status);
    /* check the completion status (here we don't care about the completion opcode */
    for (auto i = 0; i< num_entries; i++){
      if (wc_p[i].status != IBV_WC_SUCCESS)  // TODO:: could be modified into check all the entries in the array
      {
        fprintf(stderr,
                "number %d got bad completion with status: 0x%x, vendor syndrome: 0x%x\n",
                i, wc_p[i].status, wc_p[i].vendor_err);
        rc = 1;
      }
    }

  }
  return rc;
}

/******************************************************************************
* Function: print_config
*
* Input
* none
*
* Output
* none
*
* Returns
* none
*
* Description
* Print out config information
******************************************************************************/
void RDMA_Manager::print_config(void) {
  fprintf(stdout, " ------------------------------------------------\n");
  fprintf(stdout, " Device name : \"%s\"\n", rdma_config.dev_name);
  fprintf(stdout, " IB port : %u\n", rdma_config.ib_port);
  if (rdma_config.server_name)
    fprintf(stdout, " IP : %s\n", rdma_config.server_name);
  fprintf(stdout, " TCP port : %u\n", rdma_config.tcp_port);
  if (rdma_config.gid_idx >= 0)
    fprintf(stdout, " GID index : %u\n", rdma_config.gid_idx);
  fprintf(stdout, " ------------------------------------------------\n\n");
}

/******************************************************************************
* Function: usage
*
* Input
* argv0 command line arguments
*
* Output
* none
*
* Returns
* none
*
* Description
* print a description of command line syntax
******************************************************************************/
void RDMA_Manager::usage(const char* argv0) {
  fprintf(stdout, "Usage:\n");
  fprintf(stdout, " %s start a server and wait for connection\n", argv0);
  fprintf(stdout, " %s <host> connect to server at <host>\n", argv0);
  fprintf(stdout, "\n");
  fprintf(stdout, "Options:\n");
  fprintf(
      stdout,
      " -p, --port <port> listen on/connect to port <port> (default 18515)\n");
  fprintf(
      stdout,
      " -d, --ib-dev <dev> use IB device <dev> (default first device found)\n");
  fprintf(stdout,
          " -i, --ib-port <port> use port <port> of IB device (default 1)\n");
  fprintf(stdout,
          " -g, --gid_idx <git index> gid index to be used in GRH (default not used)\n");
}

bool RDMA_Manager::Remote_Memory_Register(size_t size) {
  std::unique_lock<std::shared_mutex> l(main_qp_mutex);
  // register the memory block from the remote memory
  computing_to_memory_msg* send_pointer;
  send_pointer = (computing_to_memory_msg*)res->send_buf;
  send_pointer->command = create_mr_;
  send_pointer->content.mem_size = size;
  ibv_mr* receive_pointer;
  receive_pointer = (ibv_mr*)res->receive_buf;
  post_receive<ibv_mr>(res->mr_receive, std::string("main"));
  post_send<computing_to_memory_msg>(res->mr_send, std::string("main"));
  ibv_wc wc[2] = {};
  //  while(wc.opcode != IBV_WC_RECV){
  //    poll_completion(&wc);
  //    if (wc.status != 0){
  //      fprintf(stderr, "Work completion status is %d \n", wc.status);
  //    }
  //
  //  }
  //  assert(wc.opcode == IBV_WC_RECV);
  printf("Remote memory registeration, size: %zu", size);
  if (!poll_completion(wc, 2, std::string("main"))) {  // poll the receive for 2 entires
    auto* temp_pointer = new ibv_mr();
    // Memory leak?, No, the ibv_mr pointer will be push to the remote mem pool,
    // Please remember to delete it when diregistering mem region from the remote memory
    *temp_pointer = *receive_pointer;  // create a new ibv_mr for storing the new remote memory region handler
    remote_mem_pool.push_back(temp_pointer);  // push the new pointer for the new ibv_mr (different from the receive buffer) to remote_mem_pool

    // push the bitmap of the new registed buffer to the bitmap vector in resource.
    int placeholder_num =static_cast<int>(temp_pointer->length) /(Table_Size);  // here we supposing the SSTables are 4 megabytes
    In_Use_Array* in_use_array = new In_Use_Array(placeholder_num, Table_Size, temp_pointer);
//    std::unique_lock l(remote_pool_mutex);
    Remote_Mem_Bitmap->insert({temp_pointer->addr, in_use_array});
//    l.unlock();

    // NOTICE: Couold be problematic because the pushback may not an absolute
    //   value copy. it could raise a segment fault(Double check it)
  } else {
    fprintf(stderr, "failed to poll receive for remote memory register\n");
    return false;
  }
//  l.unlock();

  return true;
}
bool RDMA_Manager::Remote_Query_Pair_Connection(std::string& qp_id) {

  create_qp(qp_id);
  union ibv_gid my_gid;
  int rc;
  ibv_qp* qp;
  if (qp_id != "")
    qp = res->qp_map.at(qp_id);
  else
    qp = static_cast<ibv_qp*>(qp_local->Get());
  if (rdma_config.gid_idx >= 0) {
    rc = ibv_query_gid(res->ib_ctx, rdma_config.ib_port, rdma_config.gid_idx,
                       &my_gid);

    if (rc) {
      fprintf(stderr, "could not get gid for port %d, index %d\n",
              rdma_config.ib_port, rdma_config.gid_idx);
      return false;
    }
  } else
    memset(&my_gid, 0, sizeof my_gid);
  std::unique_lock<std::shared_mutex> l(main_qp_mutex);
  // lock should be here because from here on we will modify the send buffer.
  //TODO: Try to understand whether this kind of memcopy without serialization is correct.
  // Could be wrong on different machine, because of the alignment
  computing_to_memory_msg* send_pointer;
  send_pointer = (computing_to_memory_msg*)res->send_buf;
  send_pointer->command = create_qp_;
  send_pointer->content.qp_config.qp_num = qp->qp_num;
//  fprintf(stdout, "QP num to be sent = 0x%x\n", qp->qp_num);
  send_pointer->content.qp_config.lid = res->port_attr.lid;
  memcpy(send_pointer->content.qp_config.gid, &my_gid, 16);
//  fprintf(stdout, "\nLocal LID = 0x%x\n", res->port_attr.lid);
  registered_qp_config* receive_pointer;
  receive_pointer = (registered_qp_config*)res->receive_buf;

  post_receive<registered_qp_config>(res->mr_receive, std::string("main"));
  post_send<computing_to_memory_msg>(res->mr_send, std::string("main"));
  ibv_wc wc[2] = {};
  //  while(wc.opcode != IBV_WC_RECV){
  //    poll_completion(&wc);
  //    if (wc.status != 0){
  //      fprintf(stderr, "Work completion status is %d \n", wc.status);
  //    }
  //
  //  }
  //  assert(wc.opcode == IBV_WC_RECV);

  if (!poll_completion(wc, 2, std::string("main"))) {
    // poll the receive for 2 entires
    registered_qp_config temp_buff = *receive_pointer;
#ifndef NDEBUG
    fprintf(stdout, "Remote QP number=0x%x\n", receive_pointer->qp_num);
    fprintf(stdout, "Remote LID = 0x%x\n", receive_pointer->lid);
#endif
    // te,p_buff will have the informatin for the remote query pair,
    // use this information for qp connection.
    connect_qp(temp_buff, qp_id);
    return true;
  } else
    return false;
  //  // sync the communication by rdma.
  //  post_receive<registered_qp_config>(receive_pointer, std::string("main"));
  //  post_send<computing_to_memory_msg>(send_pointer, std::string("main"));
  //  if(!poll_completion(wc, 2, std::string("main"))){
  //    return true;
  //  }else
  //    return false;
}

void RDMA_Manager::Allocate_Remote_RDMA_Slot(const std::string& file_name,
                                             SST_Metadata*& sst_meta) {
  // If the Remote buffer is empty, register one from the remote memory.
  sst_meta = new SST_Metadata;
  sst_meta->mr = new ibv_mr;
  if (Remote_Mem_Bitmap->empty()) {
    // this lock is to prevent the system register too much remote memory at the
    // begginning.
    std::unique_lock<std::shared_mutex> mem_write_lock(remote_mem_mutex);
    if (Remote_Mem_Bitmap->empty()) {
      Remote_Memory_Register(1 * 1024 * 1024 * 1024);
//      fs_meta_save();
    }
    mem_write_lock.unlock();
  }
  std::shared_lock<std::shared_mutex> mem_read_lock(remote_mem_mutex);
  auto ptr = Remote_Mem_Bitmap->begin();

  while (ptr != Remote_Mem_Bitmap->end()) {
    // iterate among all the remote memory region
    // find the first empty SSTable Placeholder's iterator, iterator->first is ibv_mr*
    // second is the bool vector for this ibv_mr*. Each ibv_mr is the origin block get
    // from the remote memory. The memory was divided into chunks with size == SSTable size.
    int sst_index = ptr->second->allocate_memory_slot();
    if (sst_index >= 0) {
      *(sst_meta->mr) = *((ptr->second)->get_mr_ori());
      sst_meta->mr->addr = static_cast<void*>(
          static_cast<char*>(sst_meta->mr->addr) + sst_index * Table_Size);
      sst_meta->mr->length = Table_Size;
      sst_meta->fname = file_name;
      sst_meta->map_pointer =
          (ptr->second)->get_mr_ori();  // it could be confused that the map_pointer is for the memtadata deletion
      // so that we can easily find where to deallocate our RDMA buffer. The key is a pointer to ibv_mr.
      sst_meta->file_size = 0;
#ifndef NDEBUG
//      std::cout <<"Chunk allocate at" << sst_meta->mr->addr <<"index :" << sst_index << "name: " << sst_meta->fname << std::endl;
#endif
      return;
    } else
      ptr++;
  }
  mem_read_lock.unlock();
  // If not find remote buffers are all used, allocate another remote memory region.
  std::unique_lock<std::shared_mutex> mem_write_lock(remote_mem_mutex);
  Remote_Memory_Register(1 * 1024 * 1024 * 1024);
//  fs_meta_save();
  ibv_mr* mr_last;
  mr_last = remote_mem_pool.back();
  int sst_index = Remote_Mem_Bitmap->at(mr_last->addr)->allocate_memory_slot();
  mem_write_lock.unlock();

  //  sst_meta->mr = new ibv_mr();
  *(sst_meta->mr) = *(mr_last);
  sst_meta->mr->addr = static_cast<void*>(
      static_cast<char*>(sst_meta->mr->addr) + sst_index * Table_Size);
  sst_meta->mr->length = Table_Size;
  sst_meta->fname = file_name;
  sst_meta->map_pointer = mr_last;
  return;
}
// A function try to allocat
void RDMA_Manager::Allocate_Local_RDMA_Slot(ibv_mr*& mr_input,
                                            ibv_mr*& map_pointer,
                                            std::string pool_name) {
  // allocate the RDMA slot is seperate into two situation, read and write.
  size_t chunk_size;
    chunk_size = name_to_size.at(pool_name);
  if (name_to_mem_pool.at(pool_name).empty()) {
    std::unique_lock<std::shared_mutex> mem_write_lock(local_mem_mutex);
    if (name_to_mem_pool.at(pool_name).empty()) {
      ibv_mr* mr;
      char* buff;
      Local_Memory_Register(&buff, &mr, 1024*1024*1024, pool_name);
      printf("Memory used up, Initially, allocate new one, memory pool is %s, total memory this pool is %lu\n",
             pool_name.c_str(), name_to_mem_pool.at(pool_name).size());
    }
    mem_write_lock.unlock();
  }
  std::shared_lock<std::shared_mutex> mem_read_lock(local_mem_mutex);
  auto ptr = name_to_mem_pool.at(pool_name).begin();

  while (ptr != name_to_mem_pool.at(pool_name).end()) {
//    size_t region_chunk_size = ptr->second->get_chunk_size();
//    if (region_chunk_size != chunk_size) {
//      ptr++;
//      continue;
//    }
    assert(ptr->second->get_chunk_size() == chunk_size);
    int block_index = ptr->second->allocate_memory_slot();
    if (block_index >= 0) {
      mr_input = new ibv_mr();
      map_pointer = (ptr->second)->get_mr_ori();
      *(mr_input) = *((ptr->second)->get_mr_ori());
      mr_input->addr = static_cast<void*>(static_cast<char*>(mr_input->addr) +
                                          block_index * chunk_size);
      mr_input->length = chunk_size;

      return;
    } else
      ptr++;
  }
  mem_read_lock.unlock();
  // if not find available Local block buffer then allocate a new buffer. then
  // pick up one buffer from the new Local memory region.
  // TODO:: It could happen that the local buffer size is not enough, need to reallocate a new buff again,
  // TODO:: Because there are two many thread going on at the same time.
  ibv_mr* mr_to_allocate = new ibv_mr();
  char* buff = new char[chunk_size];

  std::unique_lock<std::shared_mutex> mem_write_lock(local_mem_mutex);
  Local_Memory_Register(&buff, &mr_to_allocate, 1024*1024*1024,
                        pool_name);
  printf("Memory used up, allocate new one, memory pool is %s, total memory this pool is %lu\n",
         pool_name.c_str(), name_to_mem_pool.at(pool_name).size());

  int block_index = name_to_mem_pool.at(pool_name).at(mr_to_allocate->addr)->allocate_memory_slot();
  mem_write_lock.unlock();
  if (block_index >= 0) {
    mr_input = new ibv_mr();
    map_pointer = mr_to_allocate;
    *(mr_input) = *(mr_to_allocate);
    mr_input->addr = static_cast<void*>(static_cast<char*>(mr_input->addr) +
                                        block_index * chunk_size);
    mr_input->length = chunk_size;
    //  mr_input.fname = file_name;
    return;
  }

}
// Remeber to delete the mr because it was created be new, otherwise memory leak.
bool RDMA_Manager::Deallocate_Local_RDMA_Slot(ibv_mr* mr, ibv_mr* map_pointer,
                                              std::string buffer_type) {
  int buff_offset =
      static_cast<char*>(mr->addr) - static_cast<char*>(map_pointer->addr);
  size_t chunksize = name_to_size.at(buffer_type);
  assert(buff_offset % chunksize == 0);
  std::shared_lock<std::shared_mutex> read_lock(local_mem_mutex);
  return name_to_mem_pool.at(buffer_type).at(map_pointer->addr)
      ->deallocate_memory_slot(buff_offset / chunksize);


}
bool RDMA_Manager::Deallocate_Local_RDMA_Slot(void* p, std::string buff_type) {
  std::shared_lock<std::shared_mutex> read_lock(local_mem_mutex);
  std::map<void*, In_Use_Array*>* Bitmap;
  Bitmap = &name_to_mem_pool.at(buff_type);
  auto mr_iter = Bitmap->upper_bound(p);
  if (mr_iter == Bitmap->begin()) {
    return false;
  } else if (mr_iter == Bitmap->end()) {
    mr_iter--;
    size_t buff_offset =
        static_cast<char*>(p) - static_cast<char*>(mr_iter->first);
    //      assert(buff_offset>=0);
    if (buff_offset < mr_iter->second->get_mr_ori()->length)
      return mr_iter->second->deallocate_memory_slot(
          buff_offset / mr_iter->second->get_chunk_size());
    else
      return false;
  } else {
    mr_iter--;
    size_t buff_offset =
        static_cast<char*>(p) - static_cast<char*>(mr_iter->first);
    //      assert(buff_offset>=0);
    if (buff_offset < mr_iter->second->get_mr_ori()->length)
      return mr_iter->second->deallocate_memory_slot(
          buff_offset / mr_iter->second->get_chunk_size());
  }
  return false;
}

bool RDMA_Manager::Deallocate_Remote_RDMA_Slot(SST_Metadata* sst_meta)  {

  int buff_offset = static_cast<char*>(sst_meta->mr->addr) -
                    static_cast<char*>(sst_meta->map_pointer->addr);
  assert(buff_offset % Table_Size == 0);
#ifndef NDEBUG
//  std::cout <<"Chunk deallocate at" << sst_meta->mr->addr << "index: " << buff_offset/Table_Size << std::endl;
#endif
  std::shared_lock<std::shared_mutex> read_lock(local_mem_mutex);
  return Remote_Mem_Bitmap->at(sst_meta->map_pointer->addr)
      ->deallocate_memory_slot(buff_offset / Table_Size);
}

bool RDMA_Manager::CheckInsideLocalBuff(
    void* p,
    std::_Rb_tree_iterator<std::pair<void* const, In_Use_Array*>>& mr_iter,
    std::map<void*, In_Use_Array*>* Bitmap) {
  std::shared_lock<std::shared_mutex> read_lock(local_mem_mutex);
  if (Bitmap != nullptr){
    mr_iter = Bitmap->upper_bound(p);
    if(mr_iter == Bitmap->begin()){
      return false;
    }else if (mr_iter == Bitmap->end()){
      mr_iter--;
      size_t buff_offset = static_cast<char*>(p) - static_cast<char*>(mr_iter->first);
//      assert(buff_offset>=0);
      if (buff_offset < mr_iter->second->get_mr_ori()->length)
        return true;
      else
        return false;
    }else{
      mr_iter--;
      size_t buff_offset = static_cast<char*>(p) - static_cast<char*>(mr_iter->first);
//      assert(buff_offset>=0);
      if (buff_offset < mr_iter->second->get_mr_ori()->length)
        return true;
    }
  }else{
    //TODO: Implement a iteration to check that address in all the mempool, in case that the block size has been changed.
    return false;
  }
    return false;
}

bool RDMA_Manager::Mempool_initialize(std::string pool_name, size_t size) {

  //check whether pool name has already exist.
  if (name_to_mem_pool.find(pool_name) != name_to_mem_pool.end())
    return false;
  std::map<void*, In_Use_Array*> mem_sub_pool;
  name_to_mem_pool.insert(std::pair<std::string, std::map<void*, In_Use_Array*>>({pool_name, mem_sub_pool}));
  name_to_size.insert({pool_name, size});
  return true;
}
//serialization for Memory regions
void RDMA_Manager::mr_serialization(char*& temp, size_t& size, ibv_mr* mr){

  void* p = mr->addr;
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


}

void RDMA_Manager::fs_serialization(char*& buff, size_t& size, std::string& db_name,
                                    std::unordered_map<std::string, SST_Metadata*>& file_to_sst_meta,
    std::map<void*, In_Use_Array*>& remote_mem_bitmap){
//  auto start = std::chrono::high_resolution_clock::now();
  char* temp = buff;

  size_t namenumber = db_name.size();
  size_t namenumber_net = htonl(namenumber);
  memcpy(temp, &namenumber_net, sizeof(size_t));
  temp = temp + sizeof(size_t);

  memcpy(temp, db_name.c_str(), namenumber);
  temp = temp + namenumber;
  //serialize the filename map
  {
    size_t filenumber = file_to_sst_meta.size();
    size_t filenumber_net = htonl(filenumber);
    memcpy(temp, &filenumber_net, sizeof(size_t));
    temp = temp + sizeof(size_t);

    for (auto iter: file_to_sst_meta) {
      size_t filename_length = iter.first.size();
      size_t filename_length_net = htonl(filename_length);
      memcpy(temp, &filename_length_net, sizeof(size_t));
      temp = temp + sizeof(size_t);

      memcpy(temp, iter.first.c_str(), filename_length);
      temp = temp + filename_length;

      unsigned int file_size = iter.second->file_size;
      unsigned int file_size_net = htonl(file_size);
      memcpy(temp, &file_size_net, sizeof(unsigned int));
      temp = temp + sizeof(unsigned int);

      // check how long is the list
      SST_Metadata* meta_p = iter.second;
      SST_Metadata* temp_meta = meta_p;
      size_t list_len = 1;
      while (temp_meta->next_ptr != nullptr) {
        list_len++;
        temp_meta = temp_meta->next_ptr;
      }
      size_t list_len_net = ntohl(list_len);
      memcpy(temp, &list_len_net, sizeof(size_t));
      temp = temp + sizeof(size_t);

      meta_p = iter.second;
      size_t length_map = meta_p->map_pointer->length;
      size_t length_map_net = htonl(length_map);
      memcpy(temp, &length_map_net, sizeof(size_t));
      temp = temp + sizeof(size_t);

      //Here we put context pd handle and length outside the serialization because we do not need
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
        mr_serialization(temp, size, meta_p->mr);
        // TODO: minimize the size of the serialized data. For exe, could we save
        // TODO: the mr length only once?
        p = meta_p->map_pointer->addr;
        memcpy(temp, &p, sizeof(void*));
        temp = temp + sizeof(void*);
        meta_p = meta_p->next_ptr;

      }
    }
  }
  // Serialization for the bitmap
  size_t bitmap_number = remote_mem_bitmap.size();
  size_t bitmap_number_net = htonl(bitmap_number);
  memcpy(temp, &bitmap_number_net, sizeof(size_t));
  temp = temp + sizeof(size_t);
  for (auto iter: remote_mem_bitmap){
    void* p = iter.first;
    memcpy(temp, &p, sizeof(void*));
    temp = temp + sizeof(void*);
    size_t element_size = iter.second->get_element_size();
    size_t element_size_net = htonl(element_size);
    memcpy(temp, &element_size_net, sizeof(size_t));
    temp = temp + sizeof(size_t);
    size_t chunk_size = iter.second->get_chunk_size();
    size_t chunk_size_net = htonl(chunk_size);
    memcpy(temp, &chunk_size_net, sizeof(size_t));
    temp = temp + sizeof(size_t);
    std::list<int>* free_list = iter.second->get_free_list();
    auto mr = iter.second->get_mr_ori();
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
    mr_serialization(temp, size, iter.second->get_mr_ori());
    size_t list_size = free_list->size();
    size_t list_size_net = htonl(list_size);
    memcpy(temp, &element_size_net, sizeof(size_t));
    temp = temp + sizeof(size_t);
    for (auto index: *free_list){
      memcpy(temp, &index, sizeof(int));
      temp = temp + sizeof(int);
    }


  }
  size = temp - buff;
//  auto stop = std::chrono::high_resolution_clock::now();
//  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//  printf("fs serialization time elapse: %ld\n", duration.count());
}
void RDMA_Manager::mr_deserialization(char*& temp, size_t& size, ibv_mr*& mr){

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

  mr->addr = addr_p;
  mr->rkey = rkey;
  mr->lkey = lkey;
}
void RDMA_Manager::fs_deserilization(
    char*& buff, size_t& size, std::string& db_name,
    std::unordered_map<std::string, SST_Metadata*>& file_to_sst_meta,
    std::map<void*, In_Use_Array*>& remote_mem_bitmap, ibv_mr* local_mr) {
//  auto start = std::chrono::high_resolution_clock::now();
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
  size_t filenumber_net;
  memcpy(&filenumber_net, temp, sizeof(size_t));
  size_t filenumber = htonl(filenumber_net);
  temp = temp + sizeof(size_t);

  for (size_t i = 0; i < filenumber; i++) {
    size_t filename_length_net;
    memcpy(&filename_length_net, temp, sizeof(size_t));
    size_t filename_length = ntohl(filename_length_net);
    temp = temp + sizeof(size_t);

    char filename[filename_length+1];
    memcpy(filename, temp, filename_length);
    filename[filename_length] = '\0';
    temp = temp + filename_length;

    unsigned int file_size_net = 0;
    memcpy(&file_size_net, temp, sizeof(unsigned int));
    unsigned int file_size = ntohl(file_size_net);
    temp = temp + sizeof(unsigned int);

    size_t list_len_net = 0;
    memcpy(&list_len_net, temp, sizeof(size_t));
    size_t list_len = htonl(list_len_net);
    temp = temp + sizeof(size_t);

    SST_Metadata* meta_head;
    SST_Metadata* meta = new SST_Metadata();

    meta->file_size = file_size;

    meta_head = meta;
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
      mr_deserialization(temp, size, meta->mr);
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
    file_to_sst_meta.insert({std::string(filename), meta_head});
  }
  //desirialize the Bit map
  size_t bitmap_number_net = 0;
  memcpy(&bitmap_number_net, temp, sizeof(size_t));
  size_t bitmap_number = htonl(bitmap_number_net);
  temp = temp + sizeof(size_t);
  for (size_t i = 0; i < bitmap_number; i++){
    void* p_key;
    memcpy(&p_key, temp, sizeof(void*));
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
    mr_deserialization(temp, size, mr_inuse);
    In_Use_Array* in_use_array =
        new In_Use_Array(element_size, chunk_size, mr_inuse, true);
    auto free_list = in_use_array->get_free_list();
    size_t free_list_size_net = 0;
    memcpy(&free_list_size_net, temp, sizeof(size_t));
    size_t free_list_size = htonl(free_list_size_net);
    temp = temp + sizeof(size_t);
    int int_temp;
    for (size_t j = 0; j < free_list_size; j++){
      memcpy(&int_temp, temp, sizeof(bool));
      free_list->push_back(int_temp);
      temp = temp + sizeof(bool);
    }




    remote_mem_bitmap.insert({p_key, in_use_array});
  }
//  auto stop = std::chrono::high_resolution_clock::now();
//  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//  printf("fs pure deserialization time elapse: %ld\n", duration.count());
  ibv_dereg_mr(local_mr);
  free(buff);


}
bool RDMA_Manager::client_save_serialized_data(const std::string& db_name,
                                               char* buff, size_t buff_size,
                                               file_type type,
                                               ibv_mr* local_mr) {
//  auto start = std::chrono::high_resolution_clock::now();
  bool destroy_flag;
  if (local_mr == nullptr){
    int mr_flags =
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
    local_mr = ibv_reg_mr(res->pd, static_cast<void*>(buff), buff_size, mr_flags);
    destroy_flag = true;
  }else
    destroy_flag = false;

  std::unique_lock<std::shared_mutex> l(main_qp_mutex);
  computing_to_memory_msg* send_pointer;
  send_pointer = (computing_to_memory_msg*)res->send_buf;

  if(type == others){
    send_pointer->command = save_fs_serialized_data;
    send_pointer->content.fs_sync_cmd.data_size = buff_size;
    send_pointer->content.fs_sync_cmd.type = type;
    //sync to make sure the shared memory has post the next receive
    post_receive<char>(res->mr_receive, std::string("main"));
    // post the command for saving the serialized data.
    post_send<computing_to_memory_msg>(res->mr_send, std::string("main"));
    ibv_wc wc[2] = {};
    ibv_mr* remote_pointer;
    if (!poll_completion(wc, 2, std::string("main"))) {
      post_send(local_mr, std::string("main"), buff_size);
    }else
      fprintf(stderr, "failed to poll receive for serialized message\n");
    if (poll_completion(wc, 1, std::string("main")))
      fprintf(stderr, "failed to poll send for serialized data send\n");
//  sleep(100);
//    auto stop = std::chrono::high_resolution_clock::now();
//    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//    printf("fs meta data save communication time elapse: %ld\n", duration.count());

  }
  else if (type == log_type){
    send_pointer->command = save_log_serialized_data;
    send_pointer->content.fs_sync_cmd.data_size = buff_size;
    send_pointer->content.fs_sync_cmd.type = type;
    post_receive<int>(res->mr_receive, std::string("main"));
    post_send<computing_to_memory_msg>(res->mr_send, std::string("main"));
    ibv_wc wc[2] = {};
    ibv_mr* remote_pointer;
    poll_completion(wc, 2, std::string("main"));
    memcpy(res->send_buf, db_name.c_str(), db_name.size());
    memcpy(static_cast<char*>(res->send_buf)+db_name.size(), "\0", 1);
    //receive the size of the serialized data
    post_send(res->mr_send,"main", db_name.size()+1);
    post_send(local_mr, std::string("main"), buff_size);
    poll_completion(wc, 2, std::string("main"));
  }
  if (destroy_flag){
    ibv_dereg_mr(local_mr);
    free(buff);
  }

  return true;
}
bool RDMA_Manager::client_retrieve_serialized_data(const std::string& db_name,
                                                   char*& buff,
                                                   size_t& buff_size,
                                                   ibv_mr*& local_mr,
                                                   file_type type) {
//  auto start = std::chrono::high_resolution_clock::now();
  int mr_flags =
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
  std::unique_lock<std::shared_mutex> l(main_qp_mutex);
  ibv_wc wc[2] = {};
  computing_to_memory_msg* send_pointer;
  send_pointer = (computing_to_memory_msg*)res->send_buf;
  if (type == others){
    send_pointer->command = retrieve_fs_serialized_data;
    //sync to make sure the shared memory has post the next receive for the dbname
    post_receive<int>(res->mr_receive, std::string("main"));
    // post the command for saving the serialized data.
    post_send<computing_to_memory_msg>(res->mr_send, std::string("main"));
    if (poll_completion(wc, 2, std::string("main"))) {
      fprintf(stderr, "failed to poll receive for serialized message <retreive>\n");
      return false;
    }else
      printf("retrieve message was sent successfully\n");
    memcpy(res->send_buf, db_name.c_str(), db_name.size());
    memcpy(static_cast<char*>(res->send_buf)+db_name.size(), "\0", 1);
    //receive the size of the serialized data
    post_receive<size_t>(res->mr_receive, std::string("main"));
    post_send(res->mr_send,"main", db_name.size()+1);

    if (poll_completion(wc, 2, std::string("main"))) {
      fprintf(stderr, "failed to poll receive for serialized data size <retrieve>\n");
      return false;
    }
    buff_size = *reinterpret_cast<size_t*>(res->receive_buf);
    if (buff_size!=0){
      buff = static_cast<char*>(malloc(buff_size));
      local_mr = ibv_reg_mr(res->pd, static_cast<void*>(buff), buff_size, mr_flags);
      post_receive(local_mr,"main", buff_size);
      // send a char to tell the shared memory that this computing node is ready to receive the data
      post_send<char>(res->mr_send, std::string("main"));
      if (poll_completion(wc, 2, std::string("main"))) {
        fprintf(stderr, "failed to poll receive for serialized message\n");
        return false;
      }else{
        return true;
      }
    }
    else
      return false;

  }else if (type == log_type){
    post_receive<int>(res->mr_receive, std::string("main"));
    // post the command for saving the serialized data.
    post_send<computing_to_memory_msg>(res->mr_send, std::string("main"));
    if (poll_completion(wc, 2, std::string("main"))) {
      fprintf(stderr, "failed to poll receive for serialized message <retreive>\n");
      return false;
    }else
      printf("retrieve message was sent successfully");
    memcpy(res->send_buf, db_name.c_str(), db_name.size());
    memcpy(static_cast<char*>(res->send_buf)+db_name.size(), "\0", 1);
    //receive the size of the serialized data
    post_receive<size_t>(res->mr_receive, std::string("main"));
    post_send(res->mr_send,"main", db_name.size()+1);

    if (poll_completion(wc, 2, std::string("main"))) {
      fprintf(stderr, "failed to poll receive for serialized data size <retrieve>\n");
      return false;
    }
    buff_size = *reinterpret_cast<size_t*>(res->receive_buf);
    if (buff_size!=0){

      local_mr = log_image_mr;
      post_receive(local_mr,"main", buff_size);
      // send a char to tell the shared memory that this computing node is ready to receive the data
      post_send<char>(res->mr_send, std::string("main"));
      if (poll_completion(wc, 2, std::string("main"))) {
        fprintf(stderr, "failed to poll receive for serialized message\n");
        return false;
      }else{
        return true;
      }
    }
    else
      return false;

  }
  return true;



}

}