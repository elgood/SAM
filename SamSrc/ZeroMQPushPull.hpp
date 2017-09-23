/*
 * ZeroMQPushPull.h
 *
 *  Created on: Dec 12, 2016
 *      Author: elgood
 */

#ifndef ZEROMQ_PUSH_PULL_H
#define ZEROMQ_PUSH_PULL_H

#include <string>
#include <vector>
#include <atomic>
#include <thread>
#include <netdb.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <zmq.hpp>

#include "AbstractConsumer.hpp"
#include "BaseProducer.hpp"
#include "Netflow.hpp"


namespace sam {

class ZeroMQPushPullException : public std::runtime_error {
public:
  ZeroMQPushPullException(char const * message) : std::runtime_error(message) { } 
};

class ZeroMQPushPull : public AbstractConsumer<Netflow>, 
                       public BaseProducer<Netflow>
{
private:
  volatile bool stopPull = false; ///> Allows exit from pullThread
  size_t numNodes; ///> How many total nodes there are
  size_t nodeId; ///> The node id of this node
  std::vector<std::string> hostnames; ///> The hostnames of all the nodes
  std::vector<std::size_t> ports;  ///> The ports of all the nodes
  uint32_t hwm;  ///> The high water mark

  size_t consumeCount = 0; ///> How many items this node has seen through feed()
  size_t metricInterval = 100000; ///> How many seen before spitting metrics out

  /// The zmq context
  //std::shared_ptr<zmq::context_t> context = 
  //  std::shared_ptr<zmq::context_t>(new zmq::context_t(1));
  zmq::context_t* context = new zmq::context_t(1);

  /// A vector of all the push sockets
  std::vector<std::shared_ptr<zmq::socket_t> > pushers;

  /// A vector of all the pull sockets
  std::vector<std::shared_ptr<zmq::socket_t> > pullers;
  
  /// Keeps track of how many items have been seen.
  std::vector<std::shared_ptr<std::atomic<std::uint32_t> > > pullCounters;

  // The thread that polls the pull sockets
  std::thread pullThread;

public:
  ZeroMQPushPull(std::size_t queueLength,
                 std::size_t numNodes, 
                 std::size_t nodeId, 
                 std::vector<std::string> hostnames, 
                 std::vector<std::size_t> ports, 
                 std::size_t hwm);

  virtual ~ZeroMQPushPull()
  {
    //stopThread();
    //delete context;
    pullThread.join();
  }
  
  virtual bool consume(Netflow const& netflow);

  /**
   * Stops the pull thread, which is necessary for the program to 
   * exit when using this class.
   */
  void stopThread() {
    //stopPull = true;
    //pullThread.join();
  }

private:
  std::string getIpString(std::string hostname) const;
  
};

ZeroMQPushPull::ZeroMQPushPull(
                 std::size_t queueLength,
                 std::size_t numNodes, 
                 std::size_t nodeId, 
                 std::vector<std::string> hostnames, 
                 std::vector<std::size_t> ports, 
                 std::size_t hwm)
  :
  BaseProducer(queueLength)
{
  //std::cout << "Entering ZeroMQPushPull constructor" << std::endl; 
  this->numNodes  = numNodes;
  this->nodeId    = nodeId;
  this->hostnames = hostnames;
  this->ports     = ports;
  this->hwm       = hwm;

  std::shared_ptr<zmq::pollitem_t> items( new zmq::pollitem_t[numNodes],
    []( zmq::pollitem_t* p) { delete[] p; });
 
  for (int i =0; i < numNodes; i++) 
  {
    //std::cout << "i " << i << std::endl;
    auto counter = std::shared_ptr<std::atomic<std::uint32_t> >(
                    new std::atomic<std::uint32_t>(0));
    pullCounters.push_back( counter );   

    /////////// Adding push sockets //////////////
    auto pusher = std::shared_ptr<zmq::socket_t>(
                    new zmq::socket_t(*context, ZMQ_PUSH));

    std::string ip = getIpString(hostnames[nodeId]);
    std::string url = "tcp://" + ip + ":";
    try {  
      url = url + boost::lexical_cast<std::string>(ports[i]);
    } catch (std::exception e) {
      throw ZeroMQPushPullException(e.what()); 
    }

    // The function complains if you use std::size_t, so be sure to use the
    // uint32_t class member for hwm.
    pusher->setsockopt(ZMQ_SNDHWM, &this->hwm, sizeof(this->hwm)); 
    pusher->bind(url);
    pushers.push_back(pusher);

    //////////// Adding pull sockets //////////////
    auto puller = std::shared_ptr<zmq::socket_t>(
                    new zmq::socket_t(*context, ZMQ_PULL));

    ip = getIpString(hostnames[i]);
    url = "tcp://" + ip + ":";
    try {
      url = url + boost::lexical_cast<std::string>(ports[nodeId]);
    } catch (std::exception e) {
      throw ZeroMQPushPullException(e.what()); 
    }
    puller->setsockopt(ZMQ_RCVHWM, &this->hwm, sizeof(this->hwm));
    puller->connect(url);

    pullers.push_back(puller);

    /////////////  Adding the poll item //////////
    items.get()[i].socket = *puller;
    items.get()[i].events = ZMQ_POLLIN;
  }


  /**
   * This is the function executed by the pull thread.  The pull
   * thread is responsible for polling all the pull sockets and
   * receiving data.
   */
  auto pullFunction = [this, items]() {
    zmq::pollitem_t* pollItems = items.get();
    zmq::message_t message;
    while (!this->stopPull) {
      zmq::poll(pollItems, this->numNodes, -1);
      for (int i = 0; i < this->numNodes; i++) {
        if (pollItems[i].revents & ZMQ_POLLIN) {

          int pullCount = this->pullCounters[i]->fetch_add(1);
            std::string message1 = "pullCount " + boost::lexical_cast<std::string>(pullCount) + "\n";
            //std::cout << message1;
          this->pullers[i]->recv(&message);
          // Is this null terminated?
          char *buff = static_cast<char*>(message.data());
          std::string sNetflow(buff); 
          Netflow netflow = makeNetflow(pullCount, sNetflow);
          this->parallelFeed(netflow);
          //if (pullCount % metricInterval == 0) {
            //std::cout << "nodeid " << this->nodeId << " PullCount[" << i 
            //          << "] " << pullCount << std::endl;
          //}
        } 
      }
    }
    //std::cout << "Exiting pull function" << std::endl;
  };

  pullThread = std::thread(pullFunction); 
  
  //std::cout << "Exiting ZeroMQPushPull constructor" << std::endl; 
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
  // Test if we got the empty netflow.  If so kill the threads.
  //if (isEmptyNetflow(n)) {
  //  for (int i = 0; i < numNodes; i++) {
  //    
  //  }
  //}


  //std::cout << "ZeroMQPushPull::consume " << toString(n) << std::endl;
  // Keep track how many netflows have come through this method.
  consumeCount++;
  if (consumeCount % metricInterval == 0) {
    std::string debugMessage = "NodeId " + 
      boost::lexical_cast<std::string>(nodeId) +  " consumeCount " +
      boost::lexical_cast<std::string>(consumeCount) + "\n"; 
    printf("%s", debugMessage.c_str());
  }

  // Get the source and dest ips.  We send the netflow twice, once to each
  // node responsible for the found ips.
  std::string source = std::get<SourceIp>(n);
  std::string dest = std::get<DestIp>(n);
  size_t node1 = std::hash<std::string>{}(source) % numNodes;
  size_t node2 = std::hash<std::string>{}(dest) % numNodes;

  // Convert the netflow to a string to send over the network via zeromq.
  std::string s;
  s = toString(n);

  // The netflow was assigned a id from the previous producer.  However, we
  // want a new id to be assigned by the receiving node.  So we remove the
  // id.  The pullThread takes care of the id.
  s = removeFirstElement(s);
  //std::cout << "After removeFirstElement " << s << std::endl;

  size_t lengthString = s.size();
  zmq::message_t message1(lengthString + 1);
  snprintf ((char *) message1.data(), lengthString + 1 ,
              "%s", s.c_str());
  pushers[node1]->send(message1);

  // Don't send message twice
  if (node1 != node2) {
    zmq::message_t message2(lengthString + 1);
    snprintf ((char *) message2.data(), lengthString + 1 ,
                "%s", s.c_str());
    pushers[node2]->send(message2);
  }
  return true;
}



}
#endif
