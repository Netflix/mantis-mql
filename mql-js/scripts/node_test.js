var mql = require('./mql.js');
var assert = require("assert");

// TODO: This entire thing should be a proper test suite with functions.

//
// Test Projection
//

// Basic
var query = mql.makeQuery("basic projection query", "select e['req'], resp from stream");
var datum = {'req': {'url': 'http://www.netflix.com'}, 'resp': 'movies!', 'referrer': 'none'};
assert.deepEqual(mql.project(query, datum), {'req': {'url': 'http://www.netflix.com'}, 'resp': 'movies!'}, "Projection returns only (and all) properties requested."); 

// Nested Property
query = mql.makeQuery("nested projection query", "select e['req']['url'], resp from stream");
datum = {'req': {'url': 'http://www.netflix.com', 'method': 'get'}, 'resp': 'movies!', 'referrer': 'none'};
assert.deepEqual(mql.project(query, datum), {'req': {'url': 'http://www.netflix.com'}, 'resp': 'movies!'}, "Nested projection returns only (and all) properties requested.");

// Index Property
// TODO: It is known that negative indices don't work in JS.
query = mql.makeQuery("index projection query", "select e['commands'][1] from stream");
datum = {'commands': ["a", "b", "c"]};
assert.deepEqual(mql.project(query, datum)
    , {'commands': {1: "b"}}
    , "Index projection correctly extracts element from list.");

// Rest-of-Query
query = mql.makeQuery("rest of query", "select path from stream window 1 where method == 'get' and e['req']['url'] == 'http://www.netflix.com' group by referrer");
datum = {'req': {'url': 'http://www.netflix.com', 'method': 'get'}, 'resp': 'movies!', 'referrer': 'none', 'path': '/', 'method': 'put'};
assert.deepEqual(mql.project(query, datum)
    , {'req': {'url': 'http://www.netflix.com'}, 'referrer': 'none', 'path': '/', 'method': 'put'}
    , "Query utilizing many properties projects all necessary properties to compute results.");


var ssq1 = "select a from stream where b == 15";
var ssq2 = "select d from stream where e['z']['y'] == 10";
datum = {'a': 1, 'b': 1, 'c': 1, 'd': 1, 'x': 1, 'y': 1, 'z': {'y': 10}};
ss_projector = mql.makeSupersetProjectorMemoized(ssq1, ssq2);
assert.deepEqual(ss_projector(datum)
    , {'a': 1, 'b': 1, 'd': 1, 'z': {'y': 10}}
    , "Superset projection functions with two queries and nested lists.");


//
// Test Filtering
//

// Equality and regex
var q1 = mql.makeQuery("subid1", "SELECT e['req']['url'] from stream where e['nqOrg']=='iosui' && e['nf.cluster'] ==~ /iosui/");
assert(mql.matches(q1, {}) == false);
assert(mql.matches(q1, {'req':{'url': 'htt'}, 'nqOrg': 'iosui'}) == false);
assert(mql.matches(q1, {'req':{'url': 'htt'}, 'nqOrg': 'iosui', 'nf.cluster':'test'}) == false);
assert(mql.matches(q1, {'req':{'url': 'htt'}, 'nqOrg': 'iosui', 'nf.cluster':'iosui'}) == true);
assert(mql.matches(q1, {'req':{'url': 'htt'}, 'nqOrg': 'iosui', 'nf.cluster':'not_iosui'}) == false);

q1 = mql.makeQuery('subId1', 'SELECT * FROM stream WHERE e[\'nqOrg\'] == \'ios\' && e[\'esn\'] ==~ /.*NF.*/');
assert(mql.matches(q1, { a: 'a'}) == false);
assert(mql.matches(q1, { nqOrg: 'ios', esn: 'abc' }) == false);

// Nested Regex
q1 = mql.makeQuery('subId1', 'SELECT * FROM stream WHERE e["req"]["url"] ==~ /htt/');
assert(mql.matches(q1, {'req':{'url': 'htt'}, 'nqOrg': 'iosui'}) == true);
assert(mql.matches(q1, {'req':{'url': 'http'}, 'nqOrg': 'iosui'}) == false);

// Numeric Equality
query = mql.makeQuery('sub_numeric_equality', 'select * from stream where a == 1');
assert(mql.matches(query, {'a': 1}) == true);
assert(mql.matches(query, {'a': 11}) == false);

// Inequality
query = mql.makeQuery('numeric_gt_inequality', "select * from stream where a > 10");
assert(mql.matches(query, {'a': 1}) == false);
assert(mql.matches(query, {}) == false);
assert(mql.matches(query, {'a': 11}) == true);

query = mql.makeQuery('numeric_lt_inequality', "select * from stream where a > 10");
assert(mql.matches(query, {'a': 1}) == false);
assert(mql.matches(query, {}) == false);
assert(mql.matches(query, {'a': 11}) == true);

// Nesting
query = mql.makeQuery('conjunction_disjunction_nesting', 'select * from stream where ((a == 10 or b > 5) AND c < 25) || d == "pass"');
assert(mql.matches(query, {}) == false);
assert(mql.matches(query, {'d': 'pass'}) == true);
assert(mql.matches(query, {'c': 24, 'a': 10}) == true);
assert(mql.matches(query, {'c': 24, 'b': 6}) == true);

//
// Sampling
//
query = mql.makeQuery('random_sampling_query', 'select * from stream sample {"strategy": "RANDOM", "threshold": 200}');
query = mql.makeQuery('random_sampling_query', 'select * from stream sample {"strategy": "STICKY", "keys": ["esn"], "threshold": 200}');
