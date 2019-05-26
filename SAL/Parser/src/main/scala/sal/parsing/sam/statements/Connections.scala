package sal.parsing.sam.statements

trait Connections extends ConnectionVast
{

  // Eventually plan to support other types of connections.  This should
  // be expanded to include other connection formats and tuples.
  def connectionStatement = connectionStatementVast

}
