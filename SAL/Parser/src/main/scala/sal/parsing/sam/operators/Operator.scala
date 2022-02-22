package sal.parsing.sam.operators

/**
 * Parser for all the operators.  To add another operator, it must
 * be added to the disjunction below.
 */
trait Operator extends TopK with Sum with Average
  with Variance with SelfSimilarity with CountDistinct
{

  def operator = topKOperator |
                 ehSumOperator | sumOperator |
                 ehVarOperator | varOperator |
                 ehAveOperator | aveOperator |
                 simpleSumOperator | 
                 selfSimilarityOperator |
                 countDistinctOperator 
}
