#include <include/rocksdb/rdma.h>

#include <memory>
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
RDMA_Manager::RDMA_Manager(config_t config, std::unordered_map<ibv_mr*,
                          In_Use_Array>* Remote_Bitmap,
                          std::unordered_map<ibv_mr*, In_Use_Array>* Local_Bitmap)
    : rdma_config(config){
  res = new resources();
  res->sock = -1;
  Remote_Mem_Bitmap = Remote_Bitmap;
  Local_Mem_Bitmap = Local_Bitmap;
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
RDMA_Manager::~RDMA_Manager()
{
  if (res->qp)
    if (ibv_destroy_qp(res->qp))
    {
      fprintf(stderr, "failed to destroy QP\n");
    }

  if (res->mr_receive)
    if (ibv_dereg_mr(res->mr_receive))
    {
      fprintf(stderr, "failed to deregister MR\n");
    }
  if (res->mr_send)
    if (ibv_dereg_mr(res->mr_send))
    {
      fprintf(stderr, "failed to deregister MR\n");
    }
  if (!local_mem_pool.empty())
  {
    std::for_each(local_mem_pool.begin(), local_mem_pool.end(), ibv_dereg_mr);
    for (ibv_mr* p : local_mem_pool)
    {
       //local buffer is registered on this machine need deregistering.
      delete (char*)p->addr;
    }
    local_mem_pool.clear();
  }
  if (!remote_mem_pool.empty())
  {
    for (auto p : remote_mem_pool)
    {

      delete p; // remote buffer is not registered on this machine so just delete the structure
    }
    remote_mem_pool.clear();
  }
//  if (res->receive_buf)
//    delete res->receive_buf;
//  if (res->send_buf)
//    delete res->send_buf;
//  if (res->SST_buf)
//    delete res->SST_buf;
  if (res->cq)
    if (ibv_destroy_cq(res->cq))
    {
      fprintf(stderr, "failed to destroy CQ\n");
    }


  if (res->pd)
    if (ibv_dealloc_pd(res->pd))
    {
      fprintf(stderr, "failed to deallocate PD\n");
    }

  if (res->ib_ctx)
    if (ibv_close_device(res->ib_ctx))
    {
      fprintf(stderr, "failed to close device context\n");
    }
  if (res->sock >= 0)
    if (close(res->sock))
    {
      fprintf(stderr, "failed to close socket\n");
    }

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
int RDMA_Manager::sock_connect(const char* servername, int port)
{
	struct addrinfo* resolved_addr = NULL;
	struct addrinfo* iterator;
	char service[6];
	int sockfd = -1;
	int listenfd = 0;
	int tmp;
	struct addrinfo hints =
	{
		.ai_flags = AI_PASSIVE,
		.ai_family = AF_INET,
		.ai_socktype = SOCK_STREAM };
	if (sprintf(service, "%d", port) < 0)
		goto sock_connect_exit;
	/* Resolve DNS address, use sockfd as temp storage */
	sockfd = getaddrinfo(servername, service, &hints, &resolved_addr);
	if (sockfd < 0)
	{
		fprintf(stderr, "%s for %s:%d\n", gai_strerror(sockfd), servername, port);
		goto sock_connect_exit;
	}
	/* Search through results and find the one we want */
	for (iterator = resolved_addr; iterator; iterator = iterator->ai_next)
	{
		sockfd = socket(iterator->ai_family, iterator->ai_socktype, iterator->ai_protocol);
		if (sockfd >= 0)
		{
			if (servername) {
				/* Client mode. Initiate connection to remote */
				if ((tmp = connect(sockfd, iterator->ai_addr, iterator->ai_addrlen)))
				{
					fprintf(stdout, "failed connect \n");
					close(sockfd);
					sockfd = -1;
				}
			}
			else
			{
				/* Server mode. Set up listening socket an accept a connection */
				listenfd = sockfd;
				sockfd = -1;
				if (bind(listenfd, iterator->ai_addr, iterator->ai_addrlen))
					goto sock_connect_exit;
				listen(listenfd, 1);
				sockfd = accept(listenfd, NULL, 0);
			}
		}
	}
sock_connect_exit:
	if (listenfd)
		close(listenfd);
	if (resolved_addr)
		freeaddrinfo(resolved_addr);
	if (sockfd < 0)
	{
		if (servername)
			fprintf(stderr, "Couldn't connect to %s:%d\n", servername, port);
		else
		{
			perror("server accept");
			fprintf(stderr, "accept() failed\n");
		}
	}
	return sockfd;
}

//    register the memory through ibv_reg_mr on the local side. this function will be
//    called by both of the server side and client side.
bool RDMA_Manager::Local_Memory_Register(char** p2buffpointer, ibv_mr** p2mrpointer, size_t size){
  int mr_flags = 0;
  *p2buffpointer = new char[size];
  if (!*p2buffpointer)
  {
    fprintf(stderr, "failed to malloc bytes to memory buffer\n");
    return false;
  }
  memset(*p2buffpointer, 0, size);

  /* register the memory buffer */
  mr_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;

  *p2mrpointer = ibv_reg_mr(res->pd, *p2buffpointer, size, mr_flags);
  if (!*p2mrpointer)
  {
    fprintf(stderr, "ibv_reg_mr failed with mr_flags=0x%x, size = %zu\n", mr_flags, size);
    return false;
  }
  else{
    local_mem_pool.push_back(*p2mrpointer);
    int placeholder_num = (*p2mrpointer)->length/(Block_Size);// here we supposing the SSTables are 4 megabytes
    In_Use_Array in_use_array(placeholder_num);
    Local_Mem_Bitmap->insert({ *p2mrpointer, in_use_array });
    return true;
  }

};
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
int RDMA_Manager::resources_create()
{
	struct ibv_device** dev_list = NULL;
	struct ibv_qp_init_attr qp_init_attr;
	struct ibv_device* ib_dev = NULL;
	int iter = 1;
	int i;

	int cq_size = 0;
	int num_devices;
	int rc = 0;
	/* if client side */
        res->sock = sock_connect(rdma_config.server_name, rdma_config.tcp_port);
        if (res->sock < 0)
        {
                fprintf(stderr, "failed to establish TCP connection to server %s, port %d\n",
                        rdma_config.server_name, rdma_config.tcp_port);
                rc = -1;

        }

	fprintf(stdout, "TCP connection was established\n");
	fprintf(stdout, "searching for IB devices in host\n");
	/* get device names in the system */
	dev_list = ibv_get_device_list(&num_devices);
	if (!dev_list)
	{
		fprintf(stderr, "failed to get IB devices list\n");
		rc = 1;

	}
	/* if there isn't any IB device in host */
	if (!num_devices)
	{
		fprintf(stderr, "found %d device(s)\n", num_devices);
		rc = 1;

	}
	fprintf(stdout, "found %d device(s)\n", num_devices);
	/* search for the specific device we want to work with */
	for (i = 0; i < num_devices; i++)
	{
		if (!rdma_config.dev_name)
		{
			rdma_config.dev_name = strdup(ibv_get_device_name(dev_list[i]));
			fprintf(stdout, "device not specified, using first one found: %s\n", rdma_config.dev_name);
		}
		if (!strcmp(ibv_get_device_name(dev_list[i]), rdma_config.dev_name))
		{
			ib_dev = dev_list[i];
			break;
		}
	}
	/* if the device wasn't found in host */
	if (!ib_dev)
	{
		fprintf(stderr, "IB device %s wasn't found\n", rdma_config.dev_name);
		rc = 1;

	}
	/* get device handle */
	res->ib_ctx = ibv_open_device(ib_dev);
	if (!res->ib_ctx)
	{
		fprintf(stderr, "failed to open device %s\n", rdma_config.dev_name);
		rc = 1;

	}
	/* We are now done with device list, free it */
	ibv_free_device_list(dev_list);
	dev_list = NULL;
	ib_dev = NULL;
	/* query port properties */
	if (ibv_query_port(res->ib_ctx, rdma_config.ib_port, &res->port_attr))
	{
		fprintf(stderr, "ibv_query_port on port %u failed\n", rdma_config.ib_port);
		rc = 1;

	}
	/* allocate Protection Domain */
	res->pd = ibv_alloc_pd(res->ib_ctx);
	if (!res->pd)
	{
		fprintf(stderr, "ibv_alloc_pd failed\n");
		rc = 1;
	}
	/* each side will send only one WR, so Completion Queue with 1 entry is enough */
	cq_size = 1000;
	res->cq = ibv_create_cq(res->ib_ctx, cq_size, NULL, NULL, 0);
	if (!res->cq)
	{
		fprintf(stderr, "failed to create CQ with %u entries\n", cq_size);
		rc = 1;
	}
	/* computing node allocate SST buffers */
        if (rdma_config.server_name){
          ibv_mr* mr = new ibv_mr();
          char* buff = new char[rdma_config.init_local_buffer_size];
          if(!Local_Memory_Register(&buff, &mr, rdma_config.init_local_buffer_size)){
            fprintf(stderr, "memory registering failed by size of 0x%x\n", static_cast<unsigned>(rdma_config.init_local_buffer_size));
          }
          else{
            fprintf(stdout, "memory registering succeed by size of 0x%x\n", static_cast<unsigned>(rdma_config.init_local_buffer_size));

          }

        }
        Local_Memory_Register(&(res->send_buf), &(res->mr_send), 1000);
        Local_Memory_Register(&(res->receive_buf), &(res->mr_receive), 1000);
//        if(condition){
//          fprintf(stderr, "Local memory registering failed\n");
//
//        }


	fprintf(stdout, "SST buffer, send&receive buffer were registered with a\n");
	/* create the Queue Pair */
	memset(&qp_init_attr, 0, sizeof(qp_init_attr));
	qp_init_attr.qp_type = IBV_QPT_RC;
	qp_init_attr.sq_sig_all = 1;
	qp_init_attr.send_cq = res->cq;
	qp_init_attr.recv_cq = res->cq;
	qp_init_attr.cap.max_send_wr = 100;
	qp_init_attr.cap.max_recv_wr = 100;
	qp_init_attr.cap.max_send_sge = 30;
	qp_init_attr.cap.max_recv_sge = 30;
	res->qp = ibv_create_qp(res->pd, &qp_init_attr);
	if (!res->qp)
	{
		fprintf(stderr, "failed to create QP\n");
		rc = 1;
	}
	fprintf(stdout, "QP was created, QP number=0x%x\n", res->qp->qp_num);
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
int RDMA_Manager::sock_sync_data(int sock, int xfer_size, char* local_data, char* remote_data)
{
  int rc;
  int read_bytes = 0;
  int total_read_bytes = 0;
  rc = write(sock, local_data, xfer_size);
  if (rc < xfer_size)
    fprintf(stderr, "Failed writing data during sock_sync_data, total bytes are %d\n", rc);
  else
    rc = 0;
  while (!rc && total_read_bytes < xfer_size)
  {
    read_bytes = read(sock, remote_data, xfer_size);
    if (read_bytes > 0)
      total_read_bytes += read_bytes;
    else
      rc = read_bytes;
  }
  fprintf(stdout, "The data which has been read through is %s size is %d\n", remote_data, read_bytes);
  return rc;
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
int RDMA_Manager::connect_qp()
{
	int iter = 1;
	struct registered_qp_config local_con_data;
	struct registered_qp_config remote_con_data;
	struct registered_qp_config tmp_con_data;
	int rc = 0;
	char temp_receive[2];
        char temp_send[] = "Q";
	union ibv_gid my_gid;
	if (rdma_config.gid_idx >= 0)
	{
		rc = ibv_query_gid(res->ib_ctx, rdma_config.ib_port, rdma_config.gid_idx, &my_gid);
		if (rc)
		{
			fprintf(stderr, "could not get gid for port %d, index %d\n", rdma_config.ib_port, rdma_config.gid_idx);
			return rc;
		}
	}
	else
		memset(&my_gid, 0, sizeof my_gid);
	/* exchange using TCP sockets info required to connect QPs */

	local_con_data.qp_num = htonl(res->qp->qp_num);
	local_con_data.lid = htons(res->port_attr.lid);
	memcpy(local_con_data.gid, &my_gid, 16);
	fprintf(stdout, "\nLocal LID = 0x%x\n", res->port_attr.lid);
	if (sock_sync_data(res->sock, sizeof(struct registered_qp_config), (char*)&local_con_data, (char*)&tmp_con_data) < 0)
	{
		fprintf(stderr, "failed to exchange connection data between sides\n");
		rc = 1;
		goto connect_qp_exit;
	}
	remote_con_data.qp_num = ntohl(tmp_con_data.qp_num);
	remote_con_data.lid = ntohs(tmp_con_data.lid);
	memcpy(remote_con_data.gid, tmp_con_data.gid, 16);


	fprintf(stdout, "Remote QP number = 0x%x\n", remote_con_data.qp_num);
	fprintf(stdout, "Remote LID = 0x%x\n", remote_con_data.lid);
	if (rdma_config.gid_idx >= 0)
	{
		uint8_t* p = remote_con_data.gid;
		fprintf(stdout, "Remote GID =%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x\n ", p[0],
			p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10], p[11], p[12], p[13], p[14], p[15]);
	}
	/* modify the QP to init */
	rc = modify_qp_to_init(res->qp);
	if (rc)
	{
		fprintf(stderr, "change QP state to INIT failed\n");
		goto connect_qp_exit;
	}
	/* let the server post RR to be prepared for incoming messages */
	if (!rdma_config.server_name)
	{       ibv_mr * receive_pointer;
                receive_pointer = (ibv_mr*)res->receive_buf;
                post_receive(receive_pointer, true);

	}
	/* modify the QP to RTR */
	rc = modify_qp_to_rtr(res->qp, remote_con_data.qp_num, remote_con_data.lid, remote_con_data.gid);
	if (rc)
	{
		fprintf(stderr, "failed to modify QP state to RTR\n");
		goto connect_qp_exit;
	}
	rc = modify_qp_to_rts(res->qp);
	if (rc)
	{
		fprintf(stderr, "failed to modify QP state to RTS\n");
		goto connect_qp_exit;
	}
	fprintf(stdout, "QP state was change to RTS\n");
	/* sync to make sure that both sides are in states that they can connect to prevent packet loose */

	if (sock_sync_data(res->sock, 1, temp_send, temp_receive)) /* just send a dummy char back and forth */
	{
		fprintf(stderr, "sync error after QPs are were moved to RTS\n");
		rc = 1;
	}
connect_qp_exit:
	return rc;
}
/******************************************************************************
End of socket operations
******************************************************************************/

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
int RDMA_Manager::post_send(void* mr, bool is_server)
{
	struct ibv_send_wr sr;
	struct ibv_sge sge;
	struct ibv_send_wr* bad_wr = NULL;
	int rc;
        if(is_server){
          /* prepare the scatter/gather entry */
          memset(&sge, 0, sizeof(sge));
          sge.addr = (uintptr_t)mr;
          sge.length = sizeof(ibv_mr);
          sge.lkey = res->mr_send->lkey;
        }
        else{
          /* prepare the scatter/gather entry */
          memset(&sge, 0, sizeof(sge));
          sge.addr = (uintptr_t)res->send_buf;
          sge.length = sizeof(computing_to_memory_msg);
          sge.lkey = res->mr_send->lkey;
        }

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
	//start = std::chrono::steady_clock::now();
	rc = ibv_post_send(res->qp, &sr, &bad_wr);
	if (rc)
		fprintf(stderr, "failed to post SR\n");
	else
	{
          fprintf(stdout, "Send Request was posted\n");
	}
	return rc;
}
// return 0 means success
int RDMA_Manager::RDMA_Read(ibv_mr* remote_mr, ibv_mr* local_mr, size_t msg_size)
{
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
  sr.send_flags = IBV_SEND_SIGNALED;
  sr.wr.rdma.remote_addr = reinterpret_cast<uint64_t>(remote_mr->addr);
  sr.wr.rdma.rkey = remote_mr->rkey;

  /* there is a Receive Request in the responder side, so we won't get any into RNR flow */
  //*(start) = std::chrono::steady_clock::now();
  //start = std::chrono::steady_clock::now();
  rc = ibv_post_send(res->qp, &sr, &bad_wr);
  if (rc)
    fprintf(stderr, "failed to post SR\n");
//  else
//  {
//    fprintf(stdout, "RDMA Read Request was posted, OPCODE is %d\n", sr.opcode);
//  }
  ibv_wc wc = {};
  rc = poll_completion(&wc, 1);
  if (rc != 0)
    std::cout << "RDMA Read Failed" << std::endl;

  return rc;
}
int RDMA_Manager::RDMA_Write(ibv_mr* remote_mr, ibv_mr* local_mr, size_t msg_size)
{
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
  sr.send_flags = IBV_SEND_SIGNALED;
  sr.wr.rdma.remote_addr = reinterpret_cast<uint64_t>(remote_mr->addr);
  sr.wr.rdma.rkey = remote_mr->rkey;
  /* there is a Receive Request in the responder side, so we won't get any into RNR flow */
  //*(start) = std::chrono::steady_clock::now();
  //start = std::chrono::steady_clock::now();
  rc = ibv_post_send(res->qp, &sr, &bad_wr);
  if (rc)
    fprintf(stderr, "failed to post SR\n");
//  else
//  {
//    fprintf(stdout, "RDMA Write Request was posted, OPCODE is %d\n", sr.opcode);
//  }
  ibv_wc wc = {};
  rc = poll_completion(&wc, 1);
  if (rc != 0)
    std::cout << "RDMA Read Failed" << std::endl;
  return rc;
}
//int RDMA_Manager::post_atomic(int opcode)
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
int RDMA_Manager::post_receive(void* mr, bool is_server)
{
	struct ibv_recv_wr rr;
	struct ibv_sge sge;
	struct ibv_recv_wr* bad_wr;
	int rc;
        if(is_server){
          /* prepare the scatter/gather entry */

          memset(&sge, 0, sizeof(sge));
          sge.addr = (uintptr_t)mr;
          sge.length = sizeof(computing_to_memory_msg);
          sge.lkey = res->mr_receive->lkey;

        }
        else{
          /* prepare the scatter/gather entry */
          memset(&sge, 0, sizeof(sge));
          sge.addr = (uintptr_t)mr;
          sge.length = sizeof(ibv_mr);
          sge.lkey = res->mr_receive->lkey;
        }

	/* prepare the receive work request */
	memset(&rr, 0, sizeof(rr));
	rr.next = NULL;
	rr.wr_id = 0;
	rr.sg_list = &sge;
	rr.num_sge = 1;
	/* post the Receive Request to the RQ */
	rc = ibv_post_recv(res->qp, &rr, &bad_wr);
	if (rc)
		fprintf(stderr, "failed to post RR\n");
	else
		fprintf(stdout, "Receive Request was posted\n");
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
int RDMA_Manager::poll_completion(ibv_wc* wc_p, int num_entries) {

	//unsigned long start_time_msec;
	//unsigned long cur_time_msec;
	//struct timeval cur_time;
	int poll_result;
        int poll_num = 0;
	int rc = 0;
	/* poll the completion for a while before giving up of doing it .. */
	//gettimeofday(&cur_time, NULL);
	//start_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);
	do
	{
		poll_result = ibv_poll_cq(res->cq, num_entries, wc_p);
                if (poll_result < 0) break;
                else poll_num = poll_num + poll_result;
		/*gettimeofday(&cur_time, NULL);
		cur_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);*/
	} while (poll_num < num_entries);// && ((cur_time_msec - start_time_msec) < MAX_POLL_CQ_TIMEOUT));
	//*(end) = std::chrono::steady_clock::now();
	//end = std::chrono::steady_clock::now();
	if (poll_result < 0)
	{
		/* poll CQ failed */
		fprintf(stderr, "poll CQ failed\n");
		rc = 1;
	}
	else if (poll_result == 0)
	{ /* the CQ is empty */
		fprintf(stderr, "completion wasn't found in the CQ after timeout\n");
		rc = 1;
	}
	else
	{
		/* CQE found */
		//fprintf(stdout, "completion was found in CQ with status 0x%x\n", wc.status);
		/* check the completion status (here we don't care about the completion opcode */
		if (wc_p->status != IBV_WC_SUCCESS)// TODO:: could be modified into check all the entries in the array
		{
			fprintf(stderr, "got bad completion with status: 0x%x, vendor syndrome: 0x%x\n", wc_p->status,
				wc_p->vendor_err);
			rc = 1;
		}
	}
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
int RDMA_Manager::modify_qp_to_init(struct ibv_qp* qp)
{
	struct ibv_qp_attr attr;
	int flags;
	int rc;
	memset(&attr, 0, sizeof(attr));
	attr.qp_state = IBV_QPS_INIT;
	attr.port_num = rdma_config.ib_port;
	attr.pkey_index = 0;
	attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
	flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
	rc = ibv_modify_qp(qp, &attr, flags);
	if (rc)
		fprintf(stderr, "failed to modify QP state to INIT\n");
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
int RDMA_Manager::modify_qp_to_rtr(struct ibv_qp* qp, uint32_t remote_qpn, uint16_t dlid, uint8_t* dgid)
{
	struct ibv_qp_attr attr;
	int flags;
	int rc;
	memset(&attr, 0, sizeof(attr));
	attr.qp_state = IBV_QPS_RTR;
	attr.path_mtu = IBV_MTU_256;
	attr.dest_qp_num = remote_qpn;
	attr.rq_psn = 0;
	attr.max_dest_rd_atomic = 1;
	attr.min_rnr_timer = 0x12;
	attr.ah_attr.is_global = 0;
	attr.ah_attr.dlid = dlid;
	attr.ah_attr.sl = 0;
	attr.ah_attr.src_path_bits = 0;
	attr.ah_attr.port_num = rdma_config.ib_port;
	if (rdma_config.gid_idx >= 0)
	{
		attr.ah_attr.is_global = 1;
		attr.ah_attr.port_num = 1;
		memcpy(&attr.ah_attr.grh.dgid, dgid, 16);
		attr.ah_attr.grh.flow_label = 0;
		attr.ah_attr.grh.hop_limit = 1;
		attr.ah_attr.grh.sgid_index = rdma_config.gid_idx;
		attr.ah_attr.grh.traffic_class = 0;
	}
	flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
		IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
	rc = ibv_modify_qp(qp, &attr, flags);
	if (rc)
		fprintf(stderr, "failed to modify QP state to RTR\n");
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
int RDMA_Manager::modify_qp_to_rts(struct ibv_qp* qp)
{
	struct ibv_qp_attr attr;
	int flags;
	int rc;
	memset(&attr, 0, sizeof(attr));
	attr.qp_state = IBV_QPS_RTS;
	attr.timeout = 0x12;
	attr.retry_cnt = 6;
	attr.rnr_retry = 0;
	attr.sq_psn = 0;
	attr.max_rd_atomic = 1;
	flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
		IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;
	rc = ibv_modify_qp(qp, &attr, flags);
	if (rc)
		fprintf(stderr, "failed to modify QP state to RTS\n");
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
void RDMA_Manager::print_config(void)
{
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
void RDMA_Manager::usage(const char* argv0)
{
	fprintf(stdout, "Usage:\n");
	fprintf(stdout, " %s start a server and wait for connection\n", argv0);
	fprintf(stdout, " %s <host> connect to server at <host>\n", argv0);
	fprintf(stdout, "\n");
	fprintf(stdout, "Options:\n");
	fprintf(stdout, " -p, --port <port> listen on/connect to port <port> (default 18515)\n");
	fprintf(stdout, " -d, --ib-dev <dev> use IB device <dev> (default first device found)\n");
	fprintf(stdout, " -i, --ib-port <port> use port <port> of IB device (default 1)\n");
	fprintf(stdout, " -g, --gid_idx <git index> gid index to be used in GRH (default not used)\n");
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
void RDMA_Manager::Set_Up_RDMA(){
  int rc = 1;
  //int trans_times;
  char temp_char;
  std::string ip_add;
  std:: cin >> ip_add;
  rdma_config.server_name = ip_add.c_str();
  if (resources_create())
  {
    fprintf(stderr, "failed to create resources\n");
    return;
  }
  if (connect_qp())
  {
    fprintf(stderr, "failed to connect QPs\n");
    return;
  }

}
bool RDMA_Manager::Remote_Memory_Register(size_t size){
  //register the memory block from the remote memory
  computing_to_memory_msg * send_pointer;
  send_pointer = (computing_to_memory_msg*)res->send_buf;
  send_pointer->mem_size = size;
  ibv_mr * receive_pointer;
  receive_pointer = (ibv_mr*)res->receive_buf;
  post_receive(receive_pointer, false);
  post_send(send_pointer, false);
  ibv_wc wc[2] = {};
//  while(wc.opcode != IBV_WC_RECV){
//    poll_completion(&wc);
//    if (wc.status != 0){
//      fprintf(stderr, "Work completion status is %d \n", wc.status);
//    }
//
//  }
//  assert(wc.opcode == IBV_WC_RECV);

  if(!poll_completion(wc, 2)){ //poll the receive for 2 entires
    auto* temp_pointer = new ibv_mr();
    //Memory leak?, No, the ibv_mr pointer will be push to the remote mem pool,
    // Please remember to delete it when diregistering mem region from the remote memory
    *temp_pointer = *receive_pointer; //create a new ibv_mr for storing the new remote memory region handler
    remote_mem_pool.push_back(temp_pointer);// push the new pointer for the new ibv_mr (different from the receive buffer) to remote_mem_pool


    //push the bitmap of the new registed buffer to the bitmap vector in resource.
    int placeholder_num = static_cast<int>(temp_pointer->length)/(Table_Size);// here we supposing the SSTables are 4 megabytes
    In_Use_Array in_use_array(placeholder_num);
    Remote_Mem_Bitmap->insert({ temp_pointer, in_use_array });
    // NOTICE: Couold be problematic because the pushback may not an absolute
    //   value copy. it could raise a segment fault(Double check it)
  }
  else{
    fprintf(stderr, "failed to poll receive\n");
    return false;
  }

  return true;
}
void RDMA_Manager::Sever_thread(){

  int rc = 1;
  //int trans_times;
  char temp_char;
  if (resources_create())
  {
    fprintf(stderr, "failed to create resources\n");

  }
  if (connect_qp())
  {
    fprintf(stderr, "failed to connect QPs\n");
  }
  ibv_wc wc[2] = {};
  computing_to_memory_msg * receive_pointer;
  receive_pointer = (computing_to_memory_msg*)res->receive_buf; //copy the pointer of receive buf to a new place because
                                                                // we don't want to change the pointer type for res->receive_buff,
                                                                // making it always void.

  ibv_mr* send_pointer = (ibv_mr*)res->send_buf; // it is the same with send buff pointer.
  poll_completion(wc, 1);
  while(true){

    if(wc->opcode == IBV_WC_RECV && wc->status == IBV_WC_SUCCESS){

      ibv_mr* mr;
      char* buff = new char[receive_pointer->mem_size];
      if(!Local_Memory_Register(&buff, &mr, receive_pointer->mem_size)){
        fprintf(stderr, "memory registering failed by size of 0x%x\n", static_cast<unsigned>(receive_pointer->mem_size));
      }
//      local_mem_pool.push_back(mr);

      *send_pointer = *mr;

      post_send(send_pointer,true);
      post_receive(receive_pointer,true);

      poll_completion(wc, 1);
      poll_completion(wc, 1);


    }

    else if(wc->status != IBV_WC_SUCCESS ) {
      fprintf(stderr, "failed to poll receive\n");
    }
  }
  // TODO: Build up a exit method for shared memory side, don't forget to destroy all the RDMA resourses.


}

void RDMA_Manager::Allocate_Remote_RDMA_Slot(const std::string &file_name,
                                             SST_Metadata*& sst_meta){
  //If the Remote buffer is empty, register one from the remote memory.
  sst_meta = new SST_Metadata;
  sst_meta->mr = new ibv_mr;
  if(Remote_Mem_Bitmap->empty()){

    create_mutex.lock();
    if(Remote_Mem_Bitmap->empty()) {
      Remote_Memory_Register(1 * 1024 * 1024 * 1024);
    }
    create_mutex.unlock();
  }
  auto ptr = Remote_Mem_Bitmap->begin();

  while(ptr != Remote_Mem_Bitmap->end()) {
    // iterate among all the remote memory region
    //find the first empty SSTable Placeholder's iterator, iterator->first is ibv_mr*
    // second is the bool vector for this ibv_mr*. Each ibv_mr is the origin block get
    //from the remote memory. The memory was divided into chunks with size == SSTable size.
    int sst_index = ptr->second.allocate_memory_slot();
    if (sst_index >= 0){
      *(sst_meta->mr) = *(ptr->first);
      sst_meta->mr->addr = static_cast<void*>(static_cast<char*>(sst_meta->mr->addr) + sst_index*Table_Size);
      sst_meta->fname = file_name;
      sst_meta->map_pointer = ptr->first; // it could be confused that the map_pointer is for the memtadata deletion
      // so that we can easily find where to deallocate our RDMA buffer. The key is a pointer to ibv_mr.
      sst_meta->file_size = 0;
      return;
    } else ptr++;
  }
  // If not find remote buffers are all used, allocate another remote memory region.
  Remote_Memory_Register(1*1024*1024*1024);
  ibv_mr* mr_last;
  mr_last = remote_mem_pool.back();
  int sst_index = Remote_Mem_Bitmap->at(mr_last).allocate_memory_slot();


//  sst_meta->mr = new ibv_mr();
  *(sst_meta->mr) = *(mr_last);
  sst_meta->mr->addr = static_cast<void*>(static_cast<char*>(sst_meta->mr->addr) + sst_index*Table_Size);
  sst_meta->fname = file_name;
  sst_meta->map_pointer = ptr->first;
  return;
}
// A function try to allocat
void RDMA_Manager::Allocate_Local_RDMA_Slot(ibv_mr*& mr_input,
                                             ibv_mr*& map_pointer) {
  if(Local_Mem_Bitmap->empty()){
    ibv_mr* mr = new ibv_mr();
    char* buff = new char[Block_Size*1024];
    Local_Memory_Register(&buff, &mr, Block_Size*1024);
  }
  auto ptr = Local_Mem_Bitmap->begin();

  while(ptr != Local_Mem_Bitmap->end()) {
    int block_index = ptr->second.allocate_memory_slot();
    if (block_index >= 0){
      mr_input = new ibv_mr();
      map_pointer = ptr->first;
      *(mr_input) = *(ptr->first);
      mr_input->addr = static_cast<void*>(static_cast<char*>(mr_input->addr) + block_index*Block_Size);
      return;
    } else ptr++;
  }
  // if not find available Local block buffer then allocate a new buffer. then
  //pick up one buffer from the new Local memory region.
  // TODO:: It could happen that the local buffer size is not enough, need to reallocate a new buff again,
  // TODO:: Because there are two many thread going on at the same time.
  ibv_mr* mr_to_allocate = new ibv_mr();
  char* buff = new char[Block_Size*1024];
  Local_Memory_Register(&buff, &mr_to_allocate, Block_Size*1024);
  int block_index = ptr->second.allocate_memory_slot();
  if (block_index > 0){
    mr_input = new ibv_mr();
    map_pointer = mr_to_allocate;
    *(mr_input) = *(mr_to_allocate);
    mr_input->addr = static_cast<void*>(static_cast<char*>(mr_input->addr) + block_index*Block_Size);
//  mr_input.fname = file_name;
    return;
  }
#ifndef NDEBUG
  else {
    std::cout << "block registerration failed" << std::endl;
  }
#endif
}
// Remeber to delete the mr because it was created be new, otherwise memory leak.
bool RDMA_Manager::Deallocate_Local_RDMA_Slot(ibv_mr* mr, ibv_mr* map_pointer) const {
  int buff_offset = static_cast<char*>(mr->addr) - static_cast<char*>(map_pointer->addr);
  assert(buff_offset%Block_Size == 0);
  return Local_Mem_Bitmap->at(map_pointer).deallocate_memory_slot(buff_offset/Block_Size);
}
bool RDMA_Manager::Deallocate_Remote_RDMA_Slot(SST_Metadata* sst_meta) const {
  int buff_offset = static_cast<char*>(sst_meta->mr->addr) - static_cast<char*>(sst_meta->map_pointer->addr);
  assert(buff_offset%Table_Size == 0);
  return Remote_Mem_Bitmap->at(sst_meta->map_pointer).deallocate_memory_slot(buff_offset/Table_Size);
}

