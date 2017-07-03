#include "ZeroMQPushPull.h"
#include "Netflow.hpp"
#include <netdb.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <boost/lexical_cast.hpp>
#include <iostream>
#include <functional>


namespace sam {


ZeroMQPushPull::ZeroMQPushPull(
                 size_t queueLength,
                 size_t numNodes, 
                 size_t nodeId, 
                 std::vector<std::string> hostnames, 
                 std::vector<int> ports, 
                 uint32_t hwm)
  :
  BaseProducer(queueLength)
{
#ifdef DEBUG
  cout << "DEBUG: Entering ZeroMQPushPull constructor" << endl;
#endif
  this->numNodes  = numNodes;
  this->nodeId    = nodeId;
  this->hostnames = hostnames;
  this->ports     = ports;
  this->hwm       = hwm;

  

  std::shared_ptr<zmq::pollitem_t> items( new zmq::pollitem_t[numNodes],
    []( zmq::pollitem_t* p) { delete[] p; });
 
  for (int i =0; i < numNodes; i++) 
  {
    auto counter = std::shared_ptr<std::atomic<std::uint32_t> >(
                    new std::atomic<std::uint32_t>(0));
    pullCounters.push_back( counter );   

    /////////// Adding push sockets //////////////
    auto pusher = std::shared_ptr<zmq::socket_t>(
                    new zmq::socket_t(*context, ZMQ_PUSH));

    std::string ip = getIpString(hostnames[nodeId]);
    std::string url = "tcp://" + ip + ":" + 
                      boost::lexical_cast<std::string>(ports[i]);

    pusher->setsockopt(ZMQ_SNDHWM, &hwm, sizeof(hwm)); 
    pusher->bind(url);
    pushers.push_back(pusher);

    //////////// Adding pull sockets //////////////
    auto puller = std::shared_ptr<zmq::socket_t>(
                    new zmq::socket_t(*context, ZMQ_PULL));

    ip = getIpString(hostnames[i]);
    url = "tcp://" + ip + ":" + boost::lexical_cast<std::string>(ports[nodeId]);
    puller->setsockopt(ZMQ_RCVHWM, &hwm, sizeof(hwm));
    puller->connect(url);

    pullers.push_back(puller);

    /////////////  Adding the poll item //////////
    items.get()[i].socket = *puller;
    items.get()[i].events = ZMQ_POLLIN;
#ifdef DEBUG
    cout << "DEBUG: Leaving ZeroMQPushPull construtor" << endl;
#endif
  }


  /**
   * This is the function executed by the pull thread.  The pull
   * thread is responsible for polling all the pull sockets and
   * receiving data.
   */
  auto pullFunction = [this, items]() {
    zmq::pollitem_t* pollItems = items.get();
    zmq::message_t message;
    while (true) {
      zmq::poll(pollItems, this->numNodes, -1);
      for (int i = 0; i < this->numNodes; i++) {
        if (pollItems[i].revents & ZMQ_POLLIN) {

          int pullCount = this->pullCounters[i]->fetch_add(1);
          this->pullers[i]->recv(&message);
          // Is this null terminated?
          char *buff = static_cast<char*>(message.data());
          std::string sNetflow(buff); 
          Netflow netflow = makeNetflow(pullCount, sNetflow);
          this->parallelFeed(netflow);
          if (pullCount % metricInterval == 0) {
            std::cout << "nodeid " << this->nodeId << " PullCount[" << i 
                      << "] " << pullCount << std::endl;
          }
        } 
      }
    }
  };

  pullThread = std::thread(pullFunction); 
  
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

bool ZeroMQPushPull::consume(Netflow const& n)
{
  // Keep track how many netflows have come through this method.
  consumeCount++;
  if (consumeCount % metricInterval == 0) {
    std::cout << "NodeId " << nodeId << " consumeCount " << consumeCount 
              << std::endl; 
  }

  // Get the source and dest ips.  We send the netflow twice, once to each
  // node responsible for the found ips.
  std::string source = std::get<SourceIp>(n);
  std::string dest = std::get<DestIp>(n);
  size_t node1 = std::hash<std::string>{}(source) % numNodes;
  size_t node2 = std::hash<std::string>{}(dest) % numNodes;

  // Convert the netflow to a string to send over the network via zeromq.
  std::string s = toString(n);

  // The netflow was assigned a id from the previous producer.  However, we
  // want a new id to be assigned by the receiving node.  So we remove the
  // id.
  s = removeFirstElement(s);

  // We want to use the consumeCount as the generated id (so it is a unique
  // id for the given node).
  s = boost::lexical_cast<std::string>(consumeCount) + "," + s;

  size_t lengthString = s.size();
  zmq::message_t message1(lengthString + 1);
  snprintf ((char *) message1.data(), lengthString + 1 ,
              "%s", s.c_str());
  pushers[node1]->send(message1);
  zmq::message_t message2(lengthString + 1);
  snprintf ((char *) message2.data(), lengthString + 1 ,
              "%s", s.c_str());
  pushers[node2]->send(message2);
  return true;
}

}
