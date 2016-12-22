#include "ZeroMQPushPull.h"
#include <netdb.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <boost/lexical_cast.hpp>
#include <iostream>


namespace sam {

/**
 * This is the function executed by the pull thread.  The pull
 * thread is responsible for polling all the pull sockets and
 * receiving data.
 * \param pollItems This is a pointer to an array of poll items.
 * \param numNodes The number of nodes cooperating in the computation.
 * \param pullers The pull sockets.
 */
void pullFunction(shared_ptr<zmq::pollitem_t> pollItems,
                  size_t numNodes,
                  vector<shared_ptr<zmq::socket_t> > pullers) 
{

  zmq::pollitem_t* items = pollItems.get();
  zmq::message_t message;
  while (true) {
    zmq::poll(pollItems.get(), numNodes, -1);
    for (int i = 0; i < numNodes; i++) {
      if (items[i].revents & ZMQ_POLLIN) {
        pullers[i]->recv(&message);
        // Is this null terminated?
        char *buff = static_cast<char*>(message.data());
        string sNetflow(buff);          
      }
    }

  }

}

ZeroMQPushPull::ZeroMQPushPull(
                 size_t queueLength,
                 size_t numNodes, 
                 size_t nodeId, 
                 vector<string> hostnames, 
                 vector<int> ports, 
                 uint64_t hwm)
  :
  BaseProducer(queueLength)
{
  this->numNodes  = numNodes;
  this->nodeId    = nodeId;
  this->hostnames = hostnames;
  this->ports     = ports;
  this->hwm       = hwm;


  shared_ptr<zmq::pollitem_t> items( new zmq::pollitem_t[numNodes],
    []( zmq::pollitem_t* p) { delete[] p; });
 
  for (int i =0; i < numNodes; i++) 
  {
    auto counter = shared_ptr<atomic<std::uint32_t> >(
                    new atomic<std::uint32_t>(0));
    pullCounters.push_back( counter );   

    /////////// Adding push sockets //////////////
    auto pusher = shared_ptr<zmq::socket_t>(
                    new zmq::socket_t(*context, ZMQ_PUSH));

    std::string ip = getIpString(hostnames[nodeId]);
    std::string url = "tcp://" + ip + ":" + 
                      boost::lexical_cast<string>(ports[i]);

    std::cout << "Adding push socket to " << url << std::endl;

    pusher->setsockopt(ZMQ_SNDHWM, &hwm, sizeof(hwm)); 
    pusher->bind(url);

    pushers.push_back(pusher);

    //////////// Adding pull sockets //////////////
    auto puller = shared_ptr<zmq::socket_t>(
                    new zmq::socket_t(*context, ZMQ_PULL));

    ip = getIpString(hostnames[i]);
    url = "tcp://" + ip + ":" + boost::lexical_cast<string>(ports[nodeId]);
    puller->setsockopt(ZMQ_RCVHWM, &hwm, sizeof(hwm));
    puller->connect(url);

    pullers.push_back(puller);

    /////////////  Adding the poll item //////////
    items.get()[i].socket = *puller;
    items.get()[i].events = ZMQ_POLLIN;

  }

  pullThread = thread(pullFunction, items, numNodes, pullers); 
  
}

ZeroMQPushPull::~ZeroMQPushPull() 
{
  pullThread.join();
}

std::string ZeroMQPushPull::getIpString(std::string hostname) const
{
    hostent* hostInfo = gethostbyname(hostname.c_str());
    in_addr* address = (in_addr*)hostInfo->h_addr;
    std::string ip = inet_ntoa(* address);
    return ip;
}

}
