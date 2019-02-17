

#include <zmq.hpp>
#include <boost/lexical_cast.hpp>
#include <sam/Util.hpp>

using namespace sam;

int main(int argc, char** argv) {

  size_t startingPort = 5000;
  zmq::context_t* context = new zmq::context_t(1);

  std::string hostname = "node" + 
      boost::lexical_cast<std::string>(1);
  std::string ip = getIpString(hostname);
  std::string url = "tcp://" + ip + ":";
  url = url + boost::lexical_cast<std::string>(startingPort);

  printf("url %s\n", url.c_str());
  auto pusher = std::shared_ptr<zmq::socket_t>(
                   new zmq::socket_t(*context, ZMQ_PUSH));
  pusher->bind(url);
}
