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

#include <sam/AbstractConsumer.hpp>
#include <sam/BaseProducer.hpp>
#include <sam/Netflow.hpp>
#include <sam/IdGenerator.hpp>
#include <sam/Util.hpp>
#include <sam/ZeroMQUtil.hpp>


namespace sam {

class ZeroMQPushPullException : public std::runtime_error {
public:
  ZeroMQPushPullException(char const * message) : std::runtime_error(message) {}
  ZeroMQPushPullException(std::string message) : std::runtime_error(message) {}
};

template <typename TupleType, size_t source, size_t target, 
          typename Tuplizer, typename HF>
class ZeroMQPushPull : public AbstractConsumer<std::string>, 
                       public BaseProducer<TupleType>
{
public:
  typedef typename std::tuple_element<source, TupleType>::type SourceType;
  typedef typename std::tuple_element<target, TupleType>::type TargetType;
  typedef typename PushPull::FunctionType FunctionType;

private:
  HF hash; ///> Hashes
  Tuplizer tuplizer; ///> Converts from string to tuple
  SimpleIdGenerator idGenerator; ///> Generates unique id for each tuple
  //volatile bool stopPull = false; ///> Allows exit from pullThread
  size_t numNodes; ///> How many total nodes there are
  size_t nodeId; ///> The node id of this node
  std::vector<std::string> hostnames; ///> The hostnames of all the nodes
  //std::vector<std::size_t> ports;  ///> The ports of all the nodes
  size_t startingPort;
  bool local;
  uint32_t hwm;  ///> The high water mark
  std::atomic<bool> terminated;

  size_t consumeCount = 0; ///> How many items this node has seen through feed()
  size_t metricInterval = 100000; ///> How many seen before spitting metrics out

  /// The zmq context
  //zmq::context_t& context; // = zmq::context_t(1);

  /// A vector of all the push sockets
  //std::vector<std::shared_ptr<zmq::socket_t> > pushers;

  // The thread that polls the pull sockets
  //std::thread pullThread;

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
  ZeroMQPushPull(//zmq::context_t& context,
                 size_t queueLength,
                 size_t numNodes, 
                 size_t nodeId, 
                 std::vector<std::string> hostnames, 
                 //std::vector<std::size_t> ports, 
                 size_t startingPort,
                 size_t timeout,
                 bool local,
                 std::size_t hwm);

  virtual ~ZeroMQPushPull()
  {
    terminate();
    delete communicator;
    DEBUG_PRINT("Node %lu end of ~ZeroMQPushPull\n", nodeId);
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

  /**
   * Starts up the the pull thread to accept data.  This allows us to register
   * consumers before reading data.
   */
  //void acceptData(); 
  
  size_t getConsumeCount() const { return consumeCount; }



private:
  bool acceptingData = false;
  PushPull* communicator;
};

template <typename TupleType, size_t source, size_t target, 
          typename Tuplizer, typename HF>
ZeroMQPushPull<TupleType, source, target, Tuplizer, HF>::ZeroMQPushPull(
                 //zmq::context_t& _context,
                 size_t queueLength,
                 size_t numNodes, 
                 size_t nodeId, 
                 std::vector<std::string> hostnames, 
                 //std::vector<std::size_t> ports, 
                 size_t startingPort,
                 size_t timeout,
                 bool local,
                 size_t hwm)
  : //context(_context),
  BaseProducer<TupleType>(queueLength)
{
  this->numNodes  = numNodes;
  this->nodeId    = nodeId;
  this->hostnames = hostnames;
  this->startingPort = startingPort;
  this->local     = local;
  this->hwm       = hwm;
  terminated.store(false);

  auto callbackFunction = [this](std::string str)
  {
    size_t id = idGenerator.generate();
    TupleType tuple = tuplizer(id, str);
  
    DEBUG_PRINT("Node %lu ZeroMQPushPull pullThread received tuple "
    "%s\n", this->nodeId, sam::toString(tuple).c_str());
            
    this->parallelFeed(tuple);
  };

  // TODO make parameters of constructor
  size_t numPushSockets = 1;
  size_t numPullThreads = 1;

  std::vector<FunctionType> communicatorFunctions;
  communicatorFunctions.push_back(callbackFunction);

  communicator = new PushPull(numNodes, nodeId, numPushSockets, numPullThreads,
                              hostnames, hwm, communicatorFunctions,
                              startingPort, timeout, local); 


}

template <typename TupleType, size_t source, size_t target, 
          typename Tuplizer, typename HF>
void ZeroMQPushPull<TupleType, source, target, Tuplizer, HF>::terminate() 
{
  printf("Node %lu entering ZeroMQPushPull::terminate\n", nodeId);
  if (!terminated) {

    terminated = true;
    
    for (auto consumer : this->consumers) {
      consumer->terminate();
    }

  }

  DEBUG_PRINT("Node %lu exiting ZeroMQPushPull::terminate\n", nodeId);
}


template <typename TupleType, size_t source, size_t target, 
          typename Tuplizer, typename HF>
bool ZeroMQPushPull<TupleType, source, target, Tuplizer, HF>::
consume(std::string const& s)
{
  //if (!acceptingData) {
  //  throw ZeroMQPushPullException("ZeroMQPushPull::consume tried to consume"
  //    " an item but not accepting data yet.");
  //}

  TupleType tuple = tuplizer(0, s);

  #ifdef CHECK
  // ZeroMQPushPull::consum should only get a string without an id.
  printf("define check here\n");
  #endif

  DEBUG_PRINT("Node %lu ZeroMQPushPull::consume string %s "
   " tuple %s\n", nodeId, s.c_str(), sam::toString(tuple).c_str());

  // Keep track how many netflows have come through this method.
  consumeCount++;
  if (consumeCount % metricInterval == 0) {
    std::string debugMessage = "NodeId " + 
      boost::lexical_cast<std::string>(nodeId) +  " consumeCount " +
      boost::lexical_cast<std::string>(consumeCount) + "\n"; 
    printf("%s", debugMessage.c_str());
  }

  SourceType src = std::get<source>(tuple);
  SourceType trg = std::get<target>(tuple);

  size_t node1 = hash(src) % numNodes;
  size_t node2 = hash(trg) % numNodes;

  DEBUG_PRINT("Node %lu ZeroMQPushPull %s hash(%s) %llu hash(%s) %llu "
    "numNodes %lu node1 %lu node2 %lu\n", nodeId, s.c_str(), src.c_str(),
    hash(src), trg.c_str(), hash(trg), numNodes, node1, node2);

  if (node1 != this->nodeId) { // Don't send data to ourselves.
    //zmq::message_t message = fillZmqMessage(s);
    DEBUG_PRINT("Node %lu ZeroMQPushPull::consume because of source "
           "sending to %lu %s\n",
           nodeId, node1, s.c_str());

    //pushers[node1]->send(message);
    communicator->send(s, node1);
  } else {
    DEBUG_PRINT("Node %lu ZeroMQPushPull::consume sending to parallel "
      "feed %s\n", nodeId, s.c_str());

    uint32_t id = idGenerator.generate();
    TupleType tuple = tuplizer(0, s);
    this->parallelFeed(tuple);
  }

  // Don't send message twice
  if (node1 != node2) {
    if (node2 != this->nodeId) {  
      //zmq::message_t message = fillZmqMessage(s);

      DEBUG_PRINT("Node %lu ZeroMQPushPull::consume because of target "
        "sending to %lu %s\n", nodeId, node2, s.c_str());
      
      //pushers[node2]->send(message);
      communicator->send(s, node2);
    } else {
      DEBUG_PRINT("Node %lu ZeroMQPushPull::consume sending to parallel"
        " feed %s\n", nodeId, s.c_str());
      uint32_t id = idGenerator.generate();
      TupleType tuple = tuplizer(id, s);
      this->parallelFeed(tuple);
    }
  }
  return true;
}



}
#endif
