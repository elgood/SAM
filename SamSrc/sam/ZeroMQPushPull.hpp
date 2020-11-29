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
#include <set>
#include <sys/socket.h>
#include <zmq.hpp>

#include <sam/AbstractConsumer.hpp>
#include <sam/BaseProducer.hpp>
#include <sam/Util.hpp>
#include <sam/ZeroMQUtil.hpp>
#include <sam/tuples/Edge.hpp>


namespace sam {

class PlaceHolderClass {};

/**
 * Execption class for ZeroMQPushPull errors.
 */
class ZeroMQPushPullException : public std::runtime_error {
public:
  ZeroMQPushPullException(char const * message) : std::runtime_error(message) {}
  ZeroMQPushPullException(std::string message) : std::runtime_error(message) {}
};

/**
 * Class for partitioning tuples across the cluster.  The consumer takes
 * as input strings (both tuple and label.  The producer creates 
 * TupleType and LabelType objects.
 * @tparam TupleType The type of tuple.
 * @tparam LabelType The label type that can appear with the rest of the 
 *                   tuple.
 * @tparam Tuplizer Object that takes a string and spits out a std::tuple.
 * @tparam HF A list of hash functions that are used to partition the data.
 */
template <typename EdgeType, typename Tuplizer, typename... HF>
class ZeroMQPushPull : public AbstractConsumer<EdgeType>, 
                       public BaseProducer<EdgeType>
{
public:
  typedef typename PushPull::FunctionType FunctionType;

private:
  Tuplizer tuplizer; ///> Converts from string to tuple
  size_t numNodes; ///> How many total nodes there are
  size_t nodeId; ///> The node id of this node
  std::vector<std::string> hostnames; ///> The hostnames of all the nodes
  size_t startingPort;  ///> Starting port from which the range is created.
  bool local; ///> Indicates that we are running on one node.
  uint32_t hwm;  ///> The high water mark
  std::atomic<bool> terminated;

  size_t consumeCount = 0; ///> How many items this node has seen through feed()
  size_t metricInterval = 100000; ///> How many seen before spitting metrics out

  // Generates unique id for each tuple
  SimpleIdGenerator* idGenerator = idGenerator->getInstance(); 
    

public:

  /**
   * Constructor.
   *
   * \param queueLength This is how long the queue is in the base producer.
   *    The queue fills up and then enters a parallel loop.
   * \param numNodes The number of nodes in the cluster.
   * \param nodeId The id of this node.
   * \param hostnames The hostnames of the nodes in the cluster.
   * \param startingPort Specifies the begining of the range of ports used
   *                      by the communicator.
   * \param timeout Communicator parameter specifying how long to wait when
   *                 trying to send a message to a socket.
   * \param local Specifies that we aren't actually talking to any other nodes.
   * \param hwm The high water mark.
   */
  ZeroMQPushPull(size_t queueLength,
                 size_t numNodes, 
                 size_t nodeId, 
                 std::vector<std::string> hostnames, 
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
  
  virtual bool consume(EdgeType const& edge);
                       

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
  bool acceptingData = false;
  PushPull* communicator;

  /**
   * Compile-time base function of recursion for sending tuples along all
   * partition dimensions.  There is a tuple version to send locally and a
   * string version to send via zeromq.
   *
   * \param edge The edge to send.
   * \param s The tuple, label in string form (inside TupleLabelCombined
   *          object).
   * \param seenNodes Keeps track of which nodes have seen the tuple already.
   *
   * \tparam PlaceHolder - Just helps us determine that we are calling the base
   *   function of the recurrsion.
   */
  template<typename PlaceHolder>
  void sendTuple(EdgeType const& edge,
                 std::string const& s, 
                 std::set<int> seenNodes);

  /**
   * Compile-time recursive function of for sending tuples along all
   * partition dimensions.  There can be several hash functions defined
   * which govern how the tuples are sent across the cluster.  The recursive
   * nature of this compile-time function allows each to be called.  
   *
   * \param tuple The tuple to send.
   * \param s The tuple in string form.
   * \param seenNodes Keeps track of which nodes have seen the tuple already.
   * 
   * \tparam PlaceHolder - Just helps us disambiguate base instance from recurssive call.
   * \tparam First - The hash functon we are going to use in this invocation.
   * \tparam Rest - The rest of the hash functions used for partitioning.
   */
  template<typename PlaceHolder, typename First, typename... Rest>
  void sendTuple(EdgeType const& edge,
                 std::string const& s,
                 std::set<int> seenNodes);

};

template <typename EdgeType, typename Tuplizer, typename... HF>
ZeroMQPushPull<EdgeType, Tuplizer, HF...>::ZeroMQPushPull(
                 size_t queueLength,
                 size_t numNodes, 
                 size_t nodeId, 
                 std::vector<std::string> hostnames, 
                 size_t startingPort,
                 size_t timeout,
                 bool local,
                 size_t hwm)
  : 
  BaseProducer<EdgeType>(nodeId, queueLength)
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

    DEBUG_PRINT("Node %lu ZeroMQPushPull pullThread received tuple "
      "%s\n", this->nodeId, str.c_str());
   
    // Since we are receiving this from another node, we need to assign an
    // id to the edge. 
    size_t id = idGenerator->generate(); 
    EdgeType edge = tuplizer(id, str);
    this->parallelFeed(edge);
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

template <typename EdgeType, typename Tuplizer, typename ...HF>
void ZeroMQPushPull<EdgeType, Tuplizer, HF...>::terminate() 
{
  DEBUG_PRINT("Node %lu entering ZeroMQPushPull::terminate\n", nodeId);
  if (!terminated) {

    terminated = true;
    
    for (auto consumer : this->consumers) {
      consumer->terminate();
    }

  }

  DEBUG_PRINT("Node %lu exiting ZeroMQPushPull::terminate\n", nodeId);
}

template <typename EdgeType, typename Tuplizer, typename ...HF>
template<typename PlaceHolder>
void ZeroMQPushPull<EdgeType, Tuplizer, HF...>::sendTuple(
  EdgeType const& edge,
  std::string const& s,
  std::set<int> seenNodes)
{
}

template <typename EdgeType, typename Tuplizer, typename ...HF>
template<typename PlaceHolder, typename First, typename... Rest>
void ZeroMQPushPull<EdgeType, Tuplizer, HF...>::sendTuple(
  EdgeType const& edge,
  std::string const& s,
  std::set<int> seenNodes)
{
  First first;
  size_t node1 = first(edge.tuple) % numNodes;

  if (node1 != this->nodeId) { // Don't send data to ourselves.
           
    if (seenNodes.count(node1) == 0) {
      
      DEBUG_PRINT("Node %lu ZeroMQPushPull::consume because of source "
             "sending to %lu %s\n", nodeId, node1, s.c_str());

      seenNodes.insert(node1);
      communicator->send(s, node1);

    }
  } else {

    if (seenNodes.count(this->nodeId) == 0) {

      DEBUG_PRINT("Node %lu ZeroMQPushPull::consume sending to parallel "
        "feed %s\n", nodeId, s.c_str());

      seenNodes.insert(this->nodeId);
      this->parallelFeed(edge);
    }
  }

  sendTuple<PlaceHolderClass, Rest...>(edge, s, seenNodes);
}


template <typename EdgeType, typename Tuplizer, typename ...HF>
bool ZeroMQPushPull<EdgeType, Tuplizer, HF...>::
consume(EdgeType const& edge)
{

  std::string s = edge.toStringNoId();

  DEBUG_PRINT("Node %lu ZeroMQPushPull::consume string %s\n",
   nodeId, s.c_str());

  // Keep track how many netflows have come through this method.
  consumeCount++;
  if (consumeCount % metricInterval == 0) {
    std::string debugMessage = "NodeId " + 
      boost::lexical_cast<std::string>(nodeId) +  " consumeCount " +
      boost::lexical_cast<std::string>(consumeCount) + "\n"; 
    printf("%s", debugMessage.c_str());
  }

  std::set<int> seenNodes; ///> Keeps track of which nodes have seen the tuple.
  
  // Compile time recursive call to send the tuple along all partition
  // dimensions.
  sendTuple<PlaceHolderClass, HF...>(edge, s, seenNodes);  

  return true;
}



}
#endif
