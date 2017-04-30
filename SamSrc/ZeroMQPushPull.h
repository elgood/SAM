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

#include "AbstractConsumer.hpp"
#include "BaseProducer.hpp"
#include "Netflow.hpp"

namespace sam {


class ZeroMQPushPull : public AbstractConsumer<Netflow>, 
                       public BaseProducer<Netflow>
{
private:
  size_t numNodes; ///> How many total nodes there are
  size_t nodeId; ///> The node id of this node
  std::vector<std::string> hostnames; ///> The hostnames of all the nodes
  std::vector<int> ports;  ///> The ports of all the nodes
  uint32_t hwm;  ///> The high water mark

  size_t consumeCount = 0; ///> How many items this node has seen through feed()
  size_t metricInterval = 100000; ///> How many seen before spitting metrics out

  /// The zmq context
  std::shared_ptr<zmq::context_t> context = 
    std::shared_ptr<zmq::context_t>(new zmq::context_t(1));
  
  /// A vector of all the push sockets
  std::vector<std::shared_ptr<zmq::socket_t> > pushers;

  /// A vector of all the pull sockets
  std::vector<std::shared_ptr<zmq::socket_t> > pullers;
  
  /// Keeps track of how many items have been seen.
  std::vector<std::shared_ptr<std::atomic<std::uint32_t> > > pullCounters;

  // The thread that polls the pull sockets
  std::thread pullThread;



public:
  ZeroMQPushPull(size_t queueLength,
                 size_t numNodes, 
                 size_t nodeId, 
                 std::vector<string> hostnames, 
                 std::vector<int> ports, 
                 uint32_t hwm);

  virtual ~ZeroMQPushPull();

  virtual bool consume(Netflow const& netflow);

private:
  std::string getIpString(std::string hostname) const;
  
};


}
#endif
