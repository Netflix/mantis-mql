# Whats New in MQL
Consider this document a brief overview of the new features in MQL.
The primary objective of MQL is to enable analysis of event streams while
minimizing the number of additional features a job needs to impelement. For
example jobs previously implemented group by and windowing semantics
with additional job parameters but users can now specify this directly
within their query. This will be the driving motivation behind future features.

## Nested Field Access
Users can now access nested fields in both the select and other operators
using a similar syntax to the previous query language. For example:

select e['a']['b'], e['a']['other'] from observable where e['a']['b'] > 10

## Aggregation
Users can now perform aggregate queries using SUM, COUNT, MAX, MIN, AVERAGE.

select COUNT(e['a']['b']), MIN(e['a']['c']) from observable window 60

NOTE: Streams are required to be discretized using a WINDOW clause in order
to be able to meaningfully calculate aggregates.

NOTE: Aggregate calculation places some constraints on concurrency. In the
previous query language jobs were very scalable as mapping and filtering
are trivially parallelized. This is not true in the aggregate case for MQL.
We however have solutions for detecting the aggregate case and serializing
the Observable stream.

## From Clause
MQL uses a FROM clause which can include multiple values, ie
select * from stream_a, stream_b (Reserved for future use)

NOTE: It is recommended that Raven UI insert the "from observable" in the query,
as it is the MQL standard that the default stream be named observable.
We attempt to auto-detect and correct when it is left out however standardizing on
"from observable" will produce the fewest errors.

## Group By
Users can now perform group by directly in their queries using a group by clause.
For example:

select * from observable where e['a'] == "testValue" group by e['a']

MQL will produce an Observable of GroupedObservables with each inner observable
representing a group.

## Order By
Results can now be ordered with an ORDER BY clause. Note that this will hang
forever if the stream is not discretized using a WINDOW clause.

select * from observable order by e['deviceType']

## Window
Users can produce discrete windows of data by using the window clause it comes directly
after the FROM clause. The number specified is the number of seconds over which to window.

select * from observable window 60

MQL will continually produce windows of that length, which has the effect of batching the
data during a regular query. This is very useful for discretizing the stream for use
with aggregation.

## Limit
Users can provide a limit clause which will upper bound the number of values received
and close the stream afterwards. Useful if you want to collect a limited number of
datapoints. For example:

select * from observable limit 5000

## Other Minor Changes
- Operators can now be upper or lower case.
- MQL does not support the default "true" or "false" queries but does attempt to
  translate these to a tautological or contradictory MQL statement. For Example:
  "true" => "select * from observable"
  "false" => "select * from observable where 1 == 2"
- Sample clause now uses a strict JSON parser so be sure to double quote keys and string values.
- Conjunction and disjunction can now be done with &&, ||, and, or, AND, OR
