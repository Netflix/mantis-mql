package io.mantisrx.mql;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.mantisrx.mql.core.Query;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.Ignore;
import org.junit.Test;
import static org.assertj.core.api.Assertions.*;

@SuppressWarnings("unchecked")
public class TestComponents {

	private String eventSource = "{\n" +
		"  \"events\": [\n" +
		"    {\"path\": \"/this/is/fake\", \"success\": true, \"latency\": 123.1},\n" +
		"    {\"path\": \"/this/is/also/fake\", \"success\": true, \"latency\": 250},\n" +
		"    {\"path\": \"/all/are/fake\", \"success\": false, \"latency\": 500.22}\n" +
		"  ],\n" +
		"  \"errors\": {\n" +
		"    \"err1\": {\"message\": \"The thing you wanted to work, well it didn't\", \"code\": \"err123\"},\n" +
		"    \"err2\": {\"message\": \"The other thing, yeah it also failed.\", \"code\": \"err456\"},\n" +
		"    \"err3\": {\"message\": \"You get and error, and you get an error!\", \"code\": \"err789\"}\n" +
		"  },\n" +
		"  \"version\": \"1.0.1\",\n" +
		"  \"timestamp\": \"12345678910111213\",\n" +
		"  \"result_a\": \"SUCCESS\",\n" +
		"  \"result_b\": \"SUCCESS\",\n" +
		"  \"result_c\": \"FAILURE\"\n" +
		"}\n";

	ObjectMapper mapper = new ObjectMapper();
	private IFn require = Clojure.var("clojure.core", "require");
	private IFn cljMakeQuery;
	private IFn cljSuperset;

	private Query makeQuery(String subscriptionId, String query) {
	  return (Query)cljMakeQuery.invoke(subscriptionId, query.trim());
  }

  private IFn makeSuperSetProjector(List<String> queries) {
    return (IFn)cljSuperset.invoke(queries);
  }

  public TestComponents() {
    // Bootstrap Clojure
    require.invoke(Clojure.read("io.mantisrx.mql.core"));
    require.invoke(Clojure.read("io.mantisrx.mql.components"));
    cljMakeQuery = Clojure.var("io.mantisrx.mql.components", "make-query");
    cljSuperset = Clojure.var("io.mantisrx.mql.core", "queries->superset-projection");
  }

  // Helper for converting to Map<String, Object> cleanly.
  private Map<Object, Object> tso(Object obj) {
    return (Map<Object, Object>)obj;
  }

  @Test public void testIndexingListsInSelectWorks() throws IOException {
    Map<String, Object> event = mapper.readValue(eventSource, new TypeReference<Map<String, Object>>() {});
    Query query = makeQuery("test", "select e['events'][0] from stream");
    Map<Object, Object> projected = query.project(event);
    Map<Object, Object> result = tso((tso(projected.get("events"))).get(0L));
    assertThat(result.get("path")).isEqualTo("/this/is/fake");
    assertThat(result.get("latency")).isEqualTo(123.1);
  }

  @Test public void testNegativeIndexingListsInSelectWorks() throws IOException {
    Map<String, Object> event = mapper.readValue(eventSource, new TypeReference<Map<String, Object>>() {});
    Query query = makeQuery("test", "select e['events'][-1] from stream");
    Map<String, Object> projected = query.project(event);
    Map<Object, Object> result = tso((tso(projected.get("events"))).get(-1L));
    assertThat(result.get("path")).isEqualTo("/all/are/fake");
    assertThat(result.get("latency")).isEqualTo(500.22);
  }

  @Test public void testAnySelectWorksWithNestedListInWhereClause() throws IOException {
    Map<String, Object> event = mapper.readValue(eventSource, new TypeReference<Map<String, Object>>() {});
    Query query = makeQuery("test", "select * from stream where e['events'][*]['latency'] > 450.0");
    assertThat(query.matches(event)).isTrue();
    query = makeQuery("test", "select * from stream where e['events'][*]['latency'] > 1000.0");
    assertThat(query.matches(event)).isFalse();
  }

  @Test public void testAnySelectWorksWithNestedObjectInWhereClause() throws IOException {
    Map<String, Object> event = mapper.readValue(eventSource, new TypeReference<Map<String, Object>>() {});
    Query query = makeQuery("test", "select * from stream where e['errors'][*]['code'] = 'err456'");
    assertThat(query.matches(event)).isTrue();
    query = makeQuery("test", "select * from stream where e['errors'][*]['code'] = 'this_data_is_not_present'");
    assertThat(query.matches(event)).isFalse();
  }

  @Test public void testNestedSelectWorks() throws IOException {
    Map<String, Object> event = mapper.readValue(eventSource, new TypeReference<Map<String, Object>>() {});
    Query query = makeQuery("test", "select * from stream where e['errors']['err1']['code'] = 'err123'");
    assertThat(query.matches(event)).isTrue();
    query = makeQuery("test", "select * from stream where e['errors'][*]['code'] = 'this_data_is_not_present'");
    assertThat(query.matches(event)).isFalse();
  }


  @Test public void testPrefixSelectWorks() throws IOException {
    Map<String, Object> event = mapper.readValue(eventSource, new TypeReference<Map<String, Object>>() {});
    Query query = makeQuery("test", "select e['version'], e[^'result'] from stream");
    Map<String, Object> projected = query.project(event);
    assertThat(projected.get("version")).isEqualTo("1.0.1");
    assertThat(projected.get("result_a")).isEqualTo("SUCCESS");
    assertThat(projected.get("result_b")).isEqualTo("SUCCESS");
    assertThat(projected.get("result_c")).isEqualTo("FAILURE");
    assertThat(projected.get("timestamp")).isNull();
  }

  @Test public void testSupersetProjectionWorksInComplexInteraction() throws IOException {
    Map<String, Object> event = mapper.readValue(eventSource, new TypeReference<Map<String, Object>>() {});
    String query = "select version, e[^'result'] from stream"; // Prefix and regular property
    String query2 = "select e['version'] from stream where e['errors'][*]['code'] == 'err123'"; // Regular with star property
    String query3 = "select e['errors'][0]['code'] from stream";

    IFn ssp = makeSuperSetProjector(Arrays.asList(query, query2, query3));

    Map<String, Object> projected = (Map<String, Object>)ssp.invoke(event);
    System.out.println(projected);
    assertThat(projected.get("version")).isEqualTo("1.0.1");
    assertThat(projected.get("result_a")).isEqualTo("SUCCESS");
    assertThat(projected.get("result_b")).isEqualTo("SUCCESS");
    assertThat(projected.get("result_c")).isEqualTo("FAILURE");
    assertThat(projected.get("timestamp")).isNull();
    assertThat(projected.get("errors")).isNotNull();
  }

  @Test public void shouldFunctionForQueryOptimizedByQueryOptimizer() throws IOException {
    Map<String, Object> event = mapper.readValue(eventSource, new TypeReference<Map<String, Object>>() {});
    String query = "select * from stream where result_a ==~ /.*CC.*/";
    Query q = makeQuery("test", query);
    assertThat(q.matches(event)).isTrue();
  }
}
