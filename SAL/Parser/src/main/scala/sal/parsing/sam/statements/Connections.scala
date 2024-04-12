package sal.parsing.sam.statements

trait Connections extends ConnectionVast with ConnectionNetflowV5 with ConnectionVHS22
{

  // Eventually plan to support other types of connections.  This should
  // be expanded to include other connection formats and tuples.
  def connectionStatement = connectionStatementVast | connectionStatementNetflowV5 |
                            connectionStatementVHS22

}
