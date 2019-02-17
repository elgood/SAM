#define BOOST_TEST_MAIN TestEdge
#include <boost/test/unit_test.hpp>
#include <stdexcept>
#include <string>
#include <vector>
#include <zmq.hpp>
#include <sam/Edge.hpp>
#include <sam/Netflow.hpp>
#include <sam/NetflowGenerators.hpp>

using namespace sam;

BOOST_AUTO_TEST_CASE( test_edge )
{
  typedef Edge<Netflow, SourceIp, DestIp, TimeSeconds> EdgeType;

  UniformDestPort generator("192.168.0.1", 1);
  
  Netflow netflow1 = makeNetflow(1, generator.generate());
  Netflow netflow2 = makeNetflow(2, generator.generate());
  Netflow netflow3 = makeNetflow(3, generator.generate());
    
  EdgeType* e1 = new EdgeType(netflow1);
  EdgeType* e2 = new EdgeType(netflow2);
  EdgeType* e3 = new EdgeType(netflow3);

  e1->add(e2);
  e2->add(e3);

  EdgeType* e = e1;

  BOOST_CHECK_EQUAL(std::get<SamGeneratedId>(e->tuple), 1);
  e = e->next;
  BOOST_CHECK_EQUAL(std::get<SamGeneratedId>(e->tuple), 2);
  e = e->next;
  BOOST_CHECK_EQUAL(std::get<SamGeneratedId>(e->tuple), 3);
  e = e->next;
  BOOST_CHECK(!e);

  e2->remove();

  e = e1;
  BOOST_CHECK_EQUAL(std::get<SamGeneratedId>(e->tuple), 1);
  e = e->next;
  BOOST_CHECK_EQUAL(std::get<SamGeneratedId>(e->tuple), 3);
  e = e->next;
  BOOST_CHECK(!e);

}
