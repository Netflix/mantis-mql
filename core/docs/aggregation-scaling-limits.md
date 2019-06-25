# Aggregation Scaling Limits
MQL Queries which implement aggregates (COUNT, SUM, etc...) as well as those which use windowing must necessarily maintain state. The mantis-realtime-events library (MRE) and the source jobs do not perform MQL aggregation, consequently this takes place on the client side. This limits the blast radius should a user run a long window or an aggregate against a large amount of data.

Unfortunately this faces the same scaling problems that a stateful mapper in raven, and should be effectively treated as such.

# Details
There are two primary technical concerns in these cases:

1. Windowing must necessarily buffer the output until the end of the window.
1. Group-by cannot run multithreaded as Rx does not guarantee pairs with the same key end up on the same thread.

In the first case we're simply facing a memory issue with the requirement to buffer until the end of each window. However it is rather unusual to window output without performing some form of aggregate against the window, otherwise it is rather pointless to buffer output when it could be emitted immediately. The second case requires a single thread in order to guarantee aggregate calculations against groups will be performed together.

The naive solution is to always run MQL on a single thread, which solves the problem but is the least scalable and impacts queries that would otherwise not require this behavior -- most current queries are backward compatible and consequently do not require this behavior.

# Solutions
There are a number of approaches one can use to address this problem which primarily involve a tradeoff of engineering effort and effectiveness.

## Solution A (Implemented)
The MQL library offers a function isAggregate which is a function of String -> Boolean which will indicate wether or not a given query requires aggregate operation. The implementer can run parallel queries in the default case, and switch to a serialized mode for the aggregation step.

This represents a low effort medium effectiveness tradeoff, only queries which require the serialized behavior will suffer the performance penalty.

* It should be investigated to see if this behavior can be implemented in the MQL library itself.

## Solution B
A more scalable solution to this would be to enable MQL to emit additional formats.
An example of this would be a Mantis topology backend which could emit a multi-stage Mantis jobs that implement the desired operation in a scalable fashion. A group by stage which distributes values to downstream workers using consistent hashing would be able to scale horizontally based on the number of groups (still vertically for window size).

This solution requires significantly more engineering effort, but represents a very large increase in scalability for these queries. Currently we do not have the use cases to support such an effort, but this is the approach we will investigate should we require highly scalable aggregate queries.

NOTE: This approach would also put additional burden on Raven developers as the query would determine the Mantis artifact if this solution were to be used.
