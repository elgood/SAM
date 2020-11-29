#define BOOST_TEST_MAIN TestEdge
#include <boost/test/unit_test.hpp>
#include <stdexcept>
#include <string>
#include <vector>
#include <zmq.hpp>
#include <sam/tuples/VastNetflow.hpp>
#include <sam/tuples/VastNetflowGenerators.hpp>

using namespace sam;
using namespace sam::vast_netflow;

BOOST_AUTO_TEST_CASE( test_edge )
{
/*
  typedef Edge<VastNetflow, SourceIp, DestIp, TimeSeconds> EdgeType;

  UniformDestPort generator("192.168.0.1", 1);
  
  VastNetflow netflow1 = makeVastNetflow(generator.generate());
  VastNetflow netflow2 = makeVastNetflow(generator.generate());
  VastNetflow netflow3 = makeVastNetflow(generator.generate());
    
  EdgeType* e1 = new EdgeType(netflow1);
  EdgeType* e2 = new EdgeType(netflow2);
  EdgeType* e3 = new EdgeType(netflow3);

  e1->add(e2);
  e2->add(e3);

  EdgeType* e = e1;

  // TODO: Some equivalent test
  //BOOST_CHECK_EQUAL(std::get<SamGeneratedId>(e->tuple), 1);
  e = e->next;
  //BOOST_CHECK_EQUAL(std::get<SamGeneratedId>(e->tuple), 2);
  e = e->next;
  //BOOST_CHECK_EQUAL(std::get<SamGeneratedId>(e->tuple), 3);
  e = e->next;
  BOOST_CHECK(!e);

  e2->remove();

  e = e1;
  //BOOST_CHECK_EQUAL(std::get<SamGeneratedId>(e->tuple), 1);
  e = e->next;
  //BOOST_CHECK_EQUAL(std::get<SamGeneratedId>(e->tuple), 3);
  e = e->next;
  BOOST_CHECK(!e);
*/
}
