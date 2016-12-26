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

#include <zmq.hpp>

#include "AbstractConsumer.h"
#include "BaseProducer.h"

using std::string;
using std::size_t;
using std::vector;
using std::shared_ptr;
using std::atomic;
using std::thread;

namespace sam {


class ZeroMQPushPull : public AbstractConsumer, public BaseProducer
{
private:
  size_t queueLength; ///> The length of the input queue
  size_t numNodes; ///> How many total nodes there are
  size_t nodeId; ///> The node id of this node
  vector<string> hostnames; ///> The hostnames of all the nodes
  vector<int> ports;  ///> The ports of all the nodes
  uint32_t hwm;  ///> The high water mark

  size_t consumeCount = 0; ///> How many items this node has seen through feed()
  size_t metricInterval = 100000; ///> How many seen before spitting metrics out

  /// The zmq context
  shared_ptr<zmq::context_t> context = 
    shared_ptr<zmq::context_t>(new zmq::context_t(1));
  
  /// A vector of all the push sockets
  vector<shared_ptr<zmq::socket_t> > pushers;

  /// A vector of all the pull sockets
  vector<shared_ptr<zmq::socket_t> > pullers;
  
  /// Keeps track of how many items have been seen.
  vector<shared_ptr<atomic<std::uint32_t> > > pullCounters;

  // The thread that polls the pull sockets
  thread pullThread;



public:
  ZeroMQPushPull(size_t queueLength,
                 size_t numNodes, 
                 size_t nodeId, 
                 vector<string> hostnames, 
                 vector<int> ports, 
                 uint32_t hwm);

  ~ZeroMQPushPull();

  virtual bool consume(string s);

private:
  std::string getIpString(std::string hostname) const;
  
};


}
#endif
