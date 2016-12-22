#include <atomic>

int main()
{
  std::atomic<std::atomic<std::uint32_t> > a;
}
