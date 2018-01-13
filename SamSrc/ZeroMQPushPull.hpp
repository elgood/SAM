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
#include <sys/socket.h>
#include <zmq.hpp>

#include "AbstractConsumer.hpp"
#include "BaseProducer.hpp"
#include "Netflow.hpp"
#include "IdGenerator.hpp"
#include "Util.hpp"


namespace sam {

class ZeroMQPushPullException : public std::runtime_error {
public:
  ZeroMQPushPullException(char const * message) : std::runtime_error(message) {}
  ZeroMQPushPullException(std::string message) : std::runtime_error(message) {}
};

class ZeroMQPushPull : public AbstractConsumer<std::string>, 
                       public BaseProducer<Netflow>
{
private:
  SimpleIdGenerator idGenerator; ///> Generates unique id for each netflow
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
  //std::vector<std::shared_ptr<zmq::socket_t> > pullers;
  
  /// Keeps track of how many items have been seen.
  //std::vector<std::shared_ptr<std::atomic<std::uint32_t> > > pullCounters;

  // The thread that polls the pull sockets
  std::thread pullThread;

public:

  /**
   * Constructor.
   *
   * \param queueLength This is how long the queue is in the base producer.
   *    The queue fills up and then enters a parallel loop.
   * \param numNodes The number of nodes in the cluster.
   * \param nodeId The id of this node.
   * \param hostnames The hostnames of the nodes in the cluster.
   * \param ports The ports to connect to for each node in the cluster.
   * \param hwm The high water mark.
   */
  ZeroMQPushPull(std::size_t queueLength,
                 std::size_t numNodes, 
                 std::size_t nodeId, 
                 std::vector<std::string> hostnames, 
                 std::vector<std::size_t> ports, 
                 std::size_t hwm);

  virtual ~ZeroMQPushPull()
  {
  }
  
  virtual bool consume(std::string const& netflow);

  /** 
   * This is called by the producer that feeds this class when the producer
   * is out of data (or otherwise wants to stop computing).  This method
   * calls all the subscribing consumers of this ZeroMQPushPull class
   * and calls terminate on them, continuing the chain until the entire
   * pipeline has terminated.
   */
  void terminate();

  size_t getConsumeCount() const { return consumeCount; }


private:
  
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

  pushers.resize(numNodes);
 
  for (int i =0; i < numNodes; i++) 
  {
    if (i != nodeId) {
      //std::cout << "i " << i << std::endl;
      //auto counter = std::shared_ptr<std::atomic<std::uint32_t> >(
      //                new std::atomic<std::uint32_t>(0));
      //pullCounters.push_back( counter );   

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
      try {
        pusher->bind(url);
      } catch (std::exception e)
      {
        std::string message = "Node " + 
          boost::lexical_cast<std::string>(nodeId) +
          " couldn't bind to url " + url + ": " + e.what();
        throw ZeroMQPushPullException(message); 
      }
      pushers[i] = pusher;

      //////////// Adding pull sockets //////////////
      //auto puller = std::shared_ptr<zmq::socket_t>(
      //                new zmq::socket_t(*context, ZMQ_PULL));

      //ip = getIpString(hostnames[i]);
      //url = "tcp://" + ip + ":";
      //try {
      //  url = url + boost::lexical_cast<std::string>(ports[nodeId]);
      //} catch (std::exception e) {
      //  throw ZeroMQPushPullException(e.what()); 
      //}
      //puller->setsockopt(ZMQ_RCVHWM, &this->hwm, sizeof(this->hwm));
      //puller->connect(url);

      //pullers.push_back(puller);

      /////////////  Adding the poll item //////////
      //items.get()[i].socket = *puller;
      //items.get()[i].events = ZMQ_POLLIN;
    }
  }


  /**
   * This is the function executed by the pull thread.  The pull
   * thread is responsible for polling all the pull sockets and
   * receiving data.
   */
  auto pullFunction = [this]() {

    // All sockets passed to zmq_poll() function must belong to the same
    // thread calling zmq_poll().  Below we create the poll items and the
    // pull sockets.

    // minus 1 because we don't send things from this node to itself.
    zmq::pollitem_t pollItems[this->numNodes - 1];
    std::vector<zmq::socket_t*> sockets;

    // When a node sends a terminate flag, the corresponding entry is 
    // turned to true.  When all flags are true, the thread terminates.
    bool terminate[this->numNodes - 1];

    int numAdded = 0;
    for(int i = 0; i < this->numNodes; i++) {
      if (i != this->nodeId)
      {
        // Creating the zmq pull socket.
        zmq::socket_t* socket = new zmq::socket_t(*(this->context), ZMQ_PULL);
        std::string ip = getIpString(this->hostnames[i]);
        std::string url = "";
        url = "tcp://" + ip + ":";
        try {
          url = url + boost::lexical_cast<std::string>(
                              this->ports[this->nodeId]);
        } catch (std::exception e) {
          throw ZeroMQPushPullException(e.what());
        }
        
        socket->setsockopt(ZMQ_SNDHWM, &this->hwm, sizeof(this->hwm));
        try {
          socket->connect(url);
        } catch (std::exception e) {
          std::string message = "Couldn't connect to url " + url;
          throw ZeroMQPushPullException(message);
        }
        sockets.push_back(socket);

        pollItems[numAdded].socket = *socket;
        pollItems[numAdded].events = ZMQ_POLLIN;
        terminate[numAdded] = false;
        numAdded++;
      }
    }

    // Now we get the data from all the pull sockets through the zmq
    // poll mechanism.

    zmq::message_t message;
    bool stop = false;
  
    while (!stop) {
      int rValue = zmq::poll(pollItems, this->numNodes -1, 1);
      int numStop = 0;
      for (int i = 0; i < this->numNodes -1; i++) {
        if (pollItems[i].revents & ZMQ_POLLIN) {

          sockets[i]->recv(&message);
          if (isTerminateMessage(message)) {
            terminate[i] = true;
          } else {
            char *buff = static_cast<char*>(message.data());
            std::string sNetflow(buff); 
            uint32_t id = idGenerator.generate();
            Netflow netflow = makeNetflow(id, sNetflow);
            this->parallelFeed(netflow);
          }
        }
        if (terminate[i]) numStop++; 
      }
      if (numStop == this->numNodes - 1) stop = true;
    }

    for (auto socket : sockets) {
      delete socket;
    }
  };

  pullThread = std::thread(pullFunction); 
  
  //std::cout << "Exiting ZeroMQPushPull constructor" << std::endl; 
}


void ZeroMQPushPull::terminate() {
  
  // If terminate was called, we aren't going to receive any more
  // data, so we can push out the terminate signal to all pull sockets. 
  for(int i = 0; i < numNodes; i++) {
    if (i != nodeId) {
      zmq::message_t message = emptyZmqMessage();
      pushers[i]->send(message);
    }
  }

  // The thread running the pull sockets should terminate after 
  // receiving terminate messages from the other nodes.
  pullThread.join();
}



bool ZeroMQPushPull::consume(std::string const& s)
{
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
  std::stringstream ss(s);
  std::string item;
  std::getline(ss, item, ','); //label
  std::getline(ss, item, ','); //time
  std::getline(ss, item, ','); //time
  std::getline(ss, item, ','); //time
  std::getline(ss, item, ','); //protocol
  std::getline(ss, item, ','); //protocol
  std::getline(ss, item, ','); //source ip
  std::string source = item;
  std::getline(ss, item, ','); //dest ip
  std::string dest = item;
  size_t node1 = std::hash<std::string>{}(source) % numNodes;
  size_t node2 = std::hash<std::string>{}(dest) % numNodes;

  size_t lengthString = s.size();
  zmq::message_t message1(lengthString + 1);
  snprintf ((char *) message1.data(), lengthString + 1 ,
              "%s", s.c_str());
  if (node1 != this->nodeId) { // Don't send data to ourselves.
    pushers[node1]->send(message1);
  } else {
    uint32_t id = idGenerator.generate();
    Netflow netflow = makeNetflow(id, s);
    this->parallelFeed(netflow);
  }

  // Don't send message twice
  if (node1 != node2) {
    zmq::message_t message2(lengthString + 1);
    snprintf ((char *) message2.data(), lengthString + 1 ,
                "%s", s.c_str());
    if (node2 != this->nodeId) {  
      pushers[node2]->send(message2);
    } else {
      uint32_t id = idGenerator.generate();
      Netflow netflow = makeNetflow(id, s);
      this->parallelFeed(netflow);
    }
  }
  return true;
}



}
#endif
