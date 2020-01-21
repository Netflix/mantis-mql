(ns io.mantisrx.mql.parser
  (:require [instaparse.core :as insta]
            [instaparse.print :as iprint]))

(def mql "
QUERY = AGG_QUERY | NORMAL_QUERY
NORMAL_QUERY = SELECT (TABLE_NO_FROM | (<whitespace> TABLE)) [<whitespace> ANOMALY] [<whitespace> LIMIT] [<whitespace> ORDER] [<whitespace> SAMPLE]
AGG_QUERY = AGG_SELECT <whitespace> TABLE [<whitespace> ANOMALY] [<whitespace> HAVING] [<whitespace> ORDER] [<whitespace> LIMIT] [<whitespace> SAMPLE]
<SELECT> = <select_kw> <whitespace> (asterisk | SELECTLIST)
<AGG_SELECT> = <select_kw> <whitespace> AGG_LIST
SAMPLE = sample_kw #'.*'
AGG_LIST = (AGG_OP | operand) (<whitespace>? <','> <whitespace>? (AGG_OP | operand))*
AGG_OP = func_kw <'('> (operand | asterisk | distinct_operand) <')'> (<whitespace> as_clause)?
TABLE = FROM [<whitespace> (WINDOW | WINDOW_N)] [<whitespace> (WHERE | WHERE_TRUE | WHERE_FALSE)] [<whitespace> GROUP]
TABLE_NO_FROM = [<whitespace> (WHERE | WHERE_TRUE | WHERE_FALSE)]
SELECTLIST = operand ((<whitespace>?) <','> <whitespace>? operand)*
FROM = <from_kw> <whitespace> table_list (<whitespace> JOIN)*
JOIN = <join_kw> <whitespace> table_list  <whitespace> integer <whitespace> <seconds_kw> <whitespace> <on_kw> <whitespace> property
WINDOW = <window_kw> <whitespace> number (<whitespace> number)?
WINDOW_N = <window_kw> <whitespace> <'|'> number  <'|'>
WHERE = <where_kw> <whitespace> search_condition
WHERE_TRUE = <where_kw> <whitespace> 'true'
WHERE_FALSE = <where_kw> <whitespace> 'false'
GROUP = <group_kw> <whitespace> property (<whitespace>? <','> <whitespace>? property)*
HAVING = <having_kw> <whitespace> search_condition
ANOMALY = <anomaly_kw> <whitespace> anomaly_fn <whitespace> property <whitespace> number <whitespace> number
ORDER = <order_kw> <whitespace> property (<whitespace> desc_kw)?
SAMPLE = <sample_kw> <whitespace> sample_config
LIMIT = <limit_kw> <whitespace> number

search_condition = boolean_term | search_condition <whitespace>? or_kw <whitespace>? boolean_term
boolean_term = boolean_test | boolean_test <whitespace>? and_kw <whitespace>? boolean_test
boolean_test = StarBinaryExpr | BinaryExpr | ARegexExpr | < whitespace? '(' whitespace? > search_condition < whitespace? ')' whitespace?> | not_search_condition | boolean_term
not_search_condition = <not_kw> < whitespace? '(' whitespace?> <whitespace>? search_condition <whitespace>? < whitespace? ')' whitespace?> 
table_list = word (<whitespace>? <','> <whitespace>? word)*

<BinaryExpr> = operand <whitespace>? BINARY_OPERATOR <whitespace>? operand
StarBinaryExpr = star_property <whitespace>? BINARY_OPERATOR <whitespace>? operand
<ARegexExpr> = operand <whitespace>? REGEX_OPERATOR <whitespace>? re_expression
BINARY_OPERATOR = '=' | '==' | '!=' | '<>' | '<' | '<=' | '>' | '>=' | '==+'
REGEX_OPERATOR = '==~'

as_clause = <as_kw> <whitespace> string_literal

<operand> = sw_property | property_with_as | property | string_literal | number | boolean_literal | not_operand | null | tick
distinct_operand = <distinct_kw> <whitespace> property
null = 'null'
not_operand = <not_kw> property | <not_kw> string_literal | <not_kw> number | <not_kw> boolean_literal
string_literal = (#'\\\".*?(\\'.*?)*?\\\"' | #'\\'.*?(\\'.*?)*?\\'') (<whitespace> as_clause)?
boolean_literal = ('true' | 'false') (<whitespace> as_clause)?
re_expression = <'/'> #'([^/\\\\]*(\\\\.[^/\\\\]*)*)' <'/'>
asterisk = '*'
whitespace = #'\\s+'
word = #'[^\\,\\s]*'
q_pword = #'\"[^\"\\']+\"' | #'\\'[^\"\\']+\\''
pword = #'[^\",\\'\\s\\*\\)\\(=!<>~]+'
number = #'-?[0-9]+(\\.[0-9]+(E-?[0-9]+)?)?' (<whitespace> as_clause)?
integer = #'-?[0-9]+'
tick = 'tick()' (<whitespace> as_clause)?
property_number = #'-?[0-9]+(\\.[0-9]+(E-?[0-9]+)?)?'
property_with_as = property <whitespace> as_clause
property = pword | <'e'>(<'['> (q_pword | property_number) <']'>)+
star_property = pword | <'e'>(<'['> (q_pword | '*') <']'>)+
sw_property = <'e'>(<'['> <'^'> q_pword <']'>)
<sample_config> = #'\\{.*?\\}'

select_kw = 'select' | 'SELECT'
as_kw = 'as' | 'AS'
from_kw = 'from' | 'FROM'
join_kw = 'join' | 'JOIN'
seconds_kw = 'seconds' | 'SECONDS'
on_kw = 'on' | 'ON'
where_kw = 'where' | 'WHERE'
anomaly_kw = 'anomaly' | 'ANOMALY'
having_kw = 'having' | 'HAVING'
and_kw = 'AND' | 'and' | '&&'
or_kw = 'OR' | 'or' | '||'
sample_kw = 'sample' | 'SAMPLE'
group_kw = 'group by' | 'GROUP BY'
order_kw = 'order by' | 'ORDER BY'
limit_kw = 'limit' | 'LIMIT'
window_kw = 'window' | 'WINDOW'
func_kw = percentile_kw | 'COUNT' | 'count' | 'SUM' | 'sum' | 'MIN' | 'min' | 'MAX' | 'max' | 'AVERAGE' | 'average' | 'LIST' | 'list'
<percentile_kw> = 'P995' | 'p995' |  'P99' | 'p99' | 'P95' | 'p95' | 'P90' | 'p90' | 'P75' | 'p75' | 'P50' | 'p50' | 'P25' | 'p25' | 'P10' | 'p10' | 'P5' | 'p5' | 'P1' | 'p1'
anomaly_fn = 'mad' | 'MAD'
not_kw = 'NOT' <whitespace> | 'not' <whitespace> | '!'
distinct_kw = 'DISTINCT' | 'distinct'
desc_kw = 'DESC' | 'desc'")

(def parser (comp (insta/parser mql) clojure.string/trim))

(defn parses?
  [query]
  (->> query
       parser
       insta/failure?
       not))

(defn- reason->str
  "Provides special case for printing negative lookahead reasons"
  [r]
  (cond
    (:NOT r)
    (do (print "NOT ")
        (print (:NOT r))),
    (:char-range r)
    (iprint/char-range->str r)
    (instance? #?(:clj java.util.regex.Pattern
                  :cljs js/RegExp)
               r)
    (iprint/regexp->str r)
    :else
    r))

(defn- failure->reason-str
  [failure]
  (let
    [reasons (map reason->str (distinct (map :expecting (filter (complement :full) (:reason failure)))))]
    (str "Parse error at line: "
         (:line failure) " column: " (:column failure)
         ". Expected one of " (clojure.string/join ", " reasons))))

(defn query->parse-error
  [query]
  (if (parses? query)
    nil
    (->> query
         parser
         insta/get-failure
         failure->reason-str)))

