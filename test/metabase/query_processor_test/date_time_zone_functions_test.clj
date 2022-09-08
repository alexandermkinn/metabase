(ns metabase.query-processor-test.date-time-zone-functions-test
  (:require [clojure.test :as t]
            [metabase.test :as mt]
            [metabase.test.data :as data]
            [metabase.test.data.dataset-definitions :as defs]
            [metabase.test.data.interface :as tx])
  (:import java.time.ZonedDateTime))

(defn test-date-extract
  [expr {:keys [aggregation breakout limit fields]
         :or   {fields [[:expression "expr"]]}}]
  (if breakout
    (->> (mt/run-mbql-query times {:expressions {"expr" expr}
                                   :aggregation aggregation
                                   :limit       limit
                                   :breakout    breakout})
         (mt/formatted-rows [int int]))
    (->> (mt/run-mbql-query times {:expressions {"expr" expr}
                                   :aggregation aggregation
                                   :limit       limit
                                   :fields      fields})
         (mt/rows))))

(mt/defdataset many-times
  [["times" [{:field-name "index"
              :effective-type :type/Integer
              :base-type :type/Text}
             {:field-name "ts"
              :base-type :type/DateTime}
             {:field-name "d"
              :base-type :type/Date}
             {:field-name "t"
              :base-type :type/Time}]
    [[1 #t "2004-02-19 09:19:09" #t "2004-02-19" #t "09:19:09"]
     [2 #t "2008-06-20 10:20:10" #t "2008-06-20" #t "10:20:10"]
     [3 #t "2012-11-21 11:21:11" #t "2012-11-21" #t "11:21:11"]
     [4 #t "2012-11-21 11:21:11" #t "2012-11-21" #t "11:21:11"]]]])

(t/deftest extraction-function-tests
  (mt/test-drivers (mt/normal-drivers-with-feature :date-functions)
    (mt/dataset many-times
      (doseq [[operation col-type & tests]
              [[:get-year
                :timestamp
                [[[2004] [2008] [2012] [2012]]
                 [:get-year [:field (mt/id :times :ts) nil]]]
                [[[2004 1] [2008 1] [2012 2]]
                 [:get-year [:field (mt/id :times :ts) nil]]
                 {:aggregation [[:count]]
                  :breakout    [[:expression "expr"]]}]]

               [:get-year
                :date
                [[[2004] [2008] [2012] [2012]]
                 [:get-year [:field (mt/id :times :d) nil]]]
                [[[2004 1] [2008 1] [2012 2]]
                 [:get-year [:field (mt/id :times :d) nil]]
                 {:aggregation [[:count]]
                  :breakout    [[:expression "expr"]]}]]

               [:get-quarter
                :timestamp
                [[[1] [2] [4] [4]]
                 [:get-quarter [:field (mt/id :times :ts) nil]]]
                [[[1 1] [2 1] [4 2]]
                 [:get-quarter [:field (mt/id :times :ts) nil]]
                 {:aggregation [[:count]]
                  :breakout    [[:expression "expr"]]}]]

               [:get-quarter
                :date
                [[[1] [2] [4] [4]]
                 [:get-quarter [:field (mt/id :times :d) nil]]]
                [[[1 1] [2 1] [4 2]]
                 [:get-quarter [:field (mt/id :times :d) nil]]
                 {:aggregation [[:count]]
                  :breakout    [[:expression "expr"]]}]]

               [:get-month
                :timestamp
                [[[2] [6] [11] [11]]
                 [:get-month [:field (mt/id :times :ts) nil]]]
                [[[2 1] [6 1] [11 2]]
                 [:get-month [:field (mt/id :times :ts) nil]]
                 {:aggregation [[:count]]
                  :breakout    [[:expression "expr"]]}]]

               [:get-month
                :date
                [[[2] [6] [11] [11]]
                 [:get-month [:field (mt/id :times :d) nil]]]
                [[[2 1] [6 1] [11 2]]
                 [:get-month [:field (mt/id :times :d) nil]]
                 {:aggregation [[:count]]
                  :breakout    [[:expression "expr"]]}]]

               [:get-day
                :timestamp
                [[[19] [20] [21] [21]]
                 [:get-day [:field (mt/id :times :ts) nil]]]
                [[[19 1] [20 1] [21 2]]
                 [:get-day [:field (mt/id :times :ts) nil]]
                 {:aggregation [[:count]]
                  :breakout    [[:expression "expr"]]}]]

               [:get-day
                :date
                [[[19] [20] [21] [21]]
                 [:get-day [:field (mt/id :times :d) nil]]]
                [[[19 1] [20 1] [21 2]]
                 [:get-day [:field (mt/id :times :d) nil]]
                 {:aggregation [[:count]]
                  :breakout    [[:expression "expr"]]}]]

               [:get-day-of-week
                :timestamp
                [[[5] [6] [4] [4]]
                 [:get-day-of-week [:field (mt/id :times :ts) nil]]]
                [[[4 2] [5 1] [6 1]]
                 [:get-day-of-week [:field (mt/id :times :ts) nil]]
                 {:aggregation [[:count]]
                  :breakout    [[:expression "expr"]]}]]

               [:get-day-of-week
                :date
                [[[5] [6] [4] [4]]
                 [:get-day-of-week [:field (mt/id :times :d) nil]]]
                [[[4 2] [5 1] [6 1]]
                 [:get-day-of-week [:field (mt/id :times :d) nil]]
                 {:aggregation [[:count]]
                  :breakout    [[:expression "expr"]]}]]

               [:get-hour
                :timestamp
                [[[9] [10] [11] [11]]
                 [:get-hour [:field (mt/id :times :ts) nil]]]
                [[[9 1] [10 1] [11 2]]
                 [:get-hour [:field (mt/id :times :ts) nil]]
                 {:aggregation [[:count]]
                  :breakout    [[:expression "expr"]]}]]

               [:get-hour
                :date
                [[[0] [0] [0] [0]]
                 [:get-hour [:field (mt/id :times :d) nil]]]
                [[[0 4]]
                 [:get-hour [:field (mt/id :times :d) nil]]
                 {:aggregation [[:count]]
                  :breakout    [[:expression "expr"]]}]]

               [:get-hour
                :time
                [[[9] [10] [11] [11]]
                 [:get-hour [:field (mt/id :times :t) nil]]]
                [[[9 1] [10 1] [11 2]]
                 [:get-hour [:field (mt/id :times :t) nil]]
                 {:aggregation [[:count]]
                  :breakout    [[:expression "expr"]]}]]

               [:get-minute
                :timestamp
                [[[19] [20] [21] [21]]
                 [:get-minute [:field (mt/id :times :ts) nil]]]
                [[[19 1] [20 1] [21 2]]
                 [:get-minute [:field (mt/id :times :ts) nil]]
                 {:aggregation [[:count]]
                  :breakout    [[:expression "expr"]]}]]

               [:get-minute
                :date
                [[[0] [0] [0] [0]]
                 [:get-minute [:field (mt/id :times :d) nil]]]
                [[[0 4]]
                 [:get-minute [:field (mt/id :times :d) nil]]
                 {:aggregation [[:count]]
                  :breakout    [[:expression "expr"]]}]]

               [:get-minute
                :time
                [[[19] [20] [21] [21]]
                 [:get-minute [:field (mt/id :times :t) nil]]]
                [[[19 1] [20 1] [21 2]]
                 [:get-minute [:field (mt/id :times :t) nil]]
                 {:aggregation [[:count]]
                  :breakout    [[:expression "expr"]]}]]

               [:get-second
                :timestamp
                [[[9] [10] [11] [11]]
                 [:get-second [:field (mt/id :times :ts) nil]]]
                [[[9 1] [10 1] [11 2]]
                 [:get-second [:field (mt/id :times :ts) nil]]
                 {:aggregation [[:count]]
                  :breakout    [[:expression "expr"]]}]]

               [:get-second
                :date
                [[[0] [0] [0] [0]]
                 [:get-second [:field (mt/id :times :d) nil]]]
                [[[0 4]]
                 [:get-second [:field (mt/id :times :d) nil]]
                 {:aggregation [[:count]]
                  :breakout    [[:expression "expr"]]}]]

               [:get-second
                :time
                [[[9] [10] [11] [11]]
                 [:get-second [:field (mt/id :times :t) nil]]]
                [[[9 1] [10 1] [11 2]]
                 [:get-second [:field (mt/id :times :t) nil]]
                 {:aggregation [[:count]]
                  :breakout    [[:expression "expr"]]}]]]]
       (t/testing (format "%s function works as expected on %s column" operation col-type)
         (doseq [[expected expr more-clauses] tests]
           (t/is (= expected (test-date-extract expr more-clauses)))))))))
