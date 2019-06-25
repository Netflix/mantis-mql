# MQL Operators
This document serves as an introduction to the need-to-know information regarding the MQL operators.
We provide a syntax summary, usage examples, and a brief description of the operator.

For the mostpart these operations are implementing using their RxJava analogues:
* select -> map
* window -> window
* where -> filter
* group by -> groupBy
* order by -> orderBy
* limit -> take

For this reason you may find the ReactiveX documentation useful as well,
though it is our explicit goal to provide you with a layer of abstraction
such that you will not need to use or think about ReactiveX when writing
queries.

Where possible we've attempted to stay true to the SQL syntax to reduce
friction for new users writing queries and to allow them to leverage
their experiences with SQL.

An essential concept in MQL is the property, which can take on one of
several forms;
* property.name.here
* e["this"]["accesses"]["nested"]["properties"]
* 15
* "string literal"

These are used with most of the operators to refer to properties
within the event stream, as well as string and numeric literals.
The JSON syntax e["nested"]["properties"] can be used to access
nested fields in the events.

The last detail of note is to understand unbounded vs discrete
streams. Rx Observables which are not expected to close are an unbounded
stream and must be bounded/discretized in order for certain operators
to function with them. The window operator is useful for pratitioning
unbounded streams into discrete bounded streams for operators such as
aggregate, group by, order by.

## SELECT

Syntax: "select <property> from servo"
Examples:
  "select * from servo"
  "select nf.node from servo"
  "select e["tags"]["nf.cluster"] from servo"
  "select "string literal", 45, e["tags"]["cluster"], nf.node from servo"

The select operator allows us to project data into a new object by specifying
which properties should be carried forward into the output. Properties will
bear the same name which was used to select them. In the case of numbers
and string literals their value will also be their name. In the case of
nested properties the result will be a top-level object joined with dots.

For example the last select above would result in an object like:
{"string literal: "string literal",
 45: 45,
 "tags.cluster": "mantisagent",
 "nf.node": "i-123456"
}

Watch out for collisions on the top-level objects with dotted naems,
and the nested objects which result in top-level properties with
dotted names.

### Aggregates

Syntax: "select AGGFUNCTION(<property>) from servo"
Examples:
  "select COUNT(e["node"]) from servo window 60"
  "select COUNT(e["node"]) from servo window 60 where e["metrics"]["latency"] > 350"
  "select average(e["metrics"]["latency"]), e["node"] from servo window 10"

Supported Aggregates:
- Min
- Max
- Average
- Count
- Sum

Aggregates add analysis capabilities to MQL in order to allow us to answer
interesting questions about data in real-time. They can be intermixed
with regular properties in order to select properties and compute
information about those properties such as the example above in which
we compute average latency on a per-node bassis in 10 second windows.

NOTE: Aggregates require that the stream on which they operate
      be discretized. This can be achieved either by feeding MQL a cold
      observable in it's context or using the window operator on an
      unbounded stream.

## FROM

Syntax: "FROM <name>"
Examples:
  "select * from servo"

The FROM clause indicates to MQL the Observable from which it should draw data.
This requires some explanation, as it bears different meaning in different
contexts.

When using Raven, or directly against the queryable sources the FROM clause
refers to the source observable no matter which name is given. The operator
is in fact optional in this context and will be inserted for you if omitted.
This is for backwards compatibility reasons with mantis-query.

When using MQL as a library the name given corresponds with the names in
the context map parameter. The second parameter to eval-mql is a
Map<String, Observable<T>> and the from clause will attempt to fetch
the observable from this map.

## WINDOW

Syntax: "WINDOW <integer>"
Examples:
  "select node, latency from servo window 10"
  "select MAX(latency) from servo window 60"


The window clause divides an otherwise unbounded stream of data into discrete
time bounded streams. The integer parameter is the number of seconds over
which to perform this bounding. For example
"select * from observable window 10" will produce 10 second windows of
data.

This discretization of streams is important for use with aggregate
operations as well as group by and order by clauses which cannot
be executed and will hang on unbounded streams.

## WHERE

Syntax: "select * from servo where node == "i-123456""
Examples: 
  "select * from servo where node == "i-123456" AND e["metrics"]["latency"] > 350"
  "select * from servo where (node == "i-123456" AND e["metrics"]["latency"] > 350) OR node == "1-abcdef""
  "select * from servo where node ==~ /i-123/"

The where clause filters any events out of the stream which do not match a given predictate.
Predicates support AND / OR operations.
Binary operators supported are =, ==, <>, !=, <, <=, >, >=, ==~
The first two above are both equality, the next two are used for not equal.
The final operator ==~ can be used with a regular expression as in:
"where <property> ==~ /regex/" any Java regular expression will suffice.

## GROUP BY

Syntax: "GROUP BY <property>"
Examples:
  "select node, latency from servo where latency > 300.0 group by node"
  "select MAX(latency), e["node"] from servo group by node"
  "select MAX(latency), e["node"] from servo window 60 group by node"

Group by groups values in the output according to some property. This is
particularly useful in conjunction with aggregate operators in which
one can compute aggregate values over a group. An example of this can
be viewed above in which we calculate the maximum latency
observed for each node in 60 second windows.

NOTE: The group by clause requires that the stream on which it operates
      be discretized. This can be achieved either by feeding MQL a cold
      observable in it's context or using the window operator on an
      unbounded stream.

## ORDER BY

Syntax: "ORDER BY <property>"
Examples:
  "select node, latency from servo group by node order by latency"

The order by operator will order the results in the inner-most observable by
the specified property. For example in a query:

"select * from observable window 5 group by nf.node order by latency"

Would produce an Observable of Observables (windows) of Observables (groups).
The events within the groups would be ordered by their latency property.

NOTE: The order by clause requires that the stream on which it operates
      be discretized. This can be achieved either by feeding MQL a cold
      observable in it's context or using the window operator on an
      unbounded stream.

## LIMIT

Syntax: "LIMIT <integer>"
Example:
  "select * from servo limit 10"
  "select AVERAGE(latency) from servo window 60 limit 10"

The limit operator takes as a parameter a single integer and bounds the number
of results to <= <number>.

NOTE: Limit does NOT discretize the stream for earlier operators such as
      group by, order by, aggregates.
