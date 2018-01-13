/**
 *
 */

#ifndef EDGE_HPP
#define EDGE_HPP

template <typename TupleType, size_t source, size_t target, size_t time>
class Edge
{
public:
  Edge* next = nullptr; 
  Edge* prev = nullptr; 
  TupleType tuple;


  Edge(TupleType tuple);

  ~Edge();

  void remove();

  /**
   * Adds an edge at the end of this linked list.
   */
  void add(Edge* edge);


};

template <typename TupleType, size_t source, size_t target, size_t time>
Edge<TupleType, source, target, time>::Edge(TupleType tuple)
{
  this->tuple = tuple; 
}

template <typename TupleType, size_t source, size_t target, size_t time>
Edge<TupleType, source, target, time>::~Edge()
{

}

template <typename TupleType, size_t source, size_t target, size_t time>
void Edge<TupleType, source, target, time>::remove() 
{
  prev->next = next;
}

template <typename TupleType, size_t source, size_t target, size_t time>
void Edge<TupleType, source, target, time>::add(Edge* edge) 
{
  next = edge;
  edge->prev = this;
}




#endif
