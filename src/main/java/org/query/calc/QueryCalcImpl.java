package org.query.calc;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class QueryCalcImpl implements QueryCalc {
    @Override
    public void select(Path t1, Path t2, Path t3, Path output) throws IOException {
        // - t1 is a file contains table "t1" with two columns "a" and "x". First line is a number of rows, then each
        //  line contains exactly one row, that contains two numbers parsable by Double.parse(): value for column a and
        //  x respectively.See test resources for examples.
        // - t2 is a file contains table "t2" with columns "b" and "y". Same format.
        // - t3 is a file contains table "t3" with columns "c" and "z". Same format.
        // - output is table stored in the same format: first line is a number of rows, then each line is one row that
        //  contains two numbers: value for column a and s.
        //
        // Number of rows of all three tables lays in range [0, 1_000_000].
        // It's guaranteed that full content of all three tables fits into RAM.
        // It's guaranteed that full outer join of at least one pair (t1xt2 or t2xt3 or t1xt3) of tables can fit into RAM.
        //
        // TODO: Implement following query, put a reasonable effort into making it efficient from perspective of
        //  computation time, memory usage and resource utilization (in that exact order). You are free to use any lib
        //  from a maven central.
        //
        // SELECT a, SUM(x * y * z) AS s FROM
        // t1 LEFT JOIN (SELECT * FROM t2 JOIN t3) AS t
        // ON a < b + c
        // GROUP BY a
        // STABLE ORDER BY s DESC
        // LIMIT 10;
        //
        // Note: STABLE is not a standard SQL command. It means that you should preserve the original order.
        // In this context it means, that in case of tie on s-value you should prefer value of a, with a lower row number.
        // In case multiple occurrences, you may assume that group has a row number of the first occurrence.


        //Vishal Implementation starts 29-Nov-21


        final String separator = " ";
        TableReader tr1 = new TableReader(separator, t1);
        TableReader tr2 = new TableReader(separator, t2);
        TableReader tr3 = new TableReader(separator, t3);

        List<TableReader> allTablesList = new ArrayList<TableReader>();
        allTablesList.add(tr1);
        allTablesList.add(tr2);
        allTablesList.add(tr3);
        QueryCalcMainClass(allTablesList, output);

    }

    private void QueryCalcMainClass(List<TableReader> allTablesList, Path output) {
        ExecutorService fileReaderExecutor = Executors.newFixedThreadPool(3);
        List<Future<double[][]>> futures;
        try {
            futures = fileReaderExecutor.invokeAll(allTablesList);
            try {

                double[][] table1 = null;
                double[][] table2 = null;
                double[][] table3 = null;
                if (futures.get(0) != null && futures.size() > 0)
                    table1 = futures.get(0).get();
                if (futures.get(1) != null && futures.size() > 1)
                    table2 = futures.get(1).get();
                if (futures.get(2) != null && futures.size() > 2)
                    table3 = futures.get(2).get();
                fileReaderExecutor.shutdown();
                fileReaderExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);


                double[][] tableTwoThree = null;
                if (table2 != null && table3 != null) {
                    tableTwoThree = multiplyTableTwoThree(table2, table3);
                    LinkedHashMap<Double, Double> expectedResultMap = null;
                        expectedResultMap = multiplyTableOneTwoThree(table1, tableTwoThree);
                        writeExpectedResult(sortMapFunction(expectedResultMap), output, table1);
                    }
            } catch (ExecutionException | IOException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }

        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }


    private double[][] multiplyTableTwoThree(double[][] tableTwo, double[][] tableThree) {
        int rows = tableTwo.length * tableThree.length;
        int columns = tableTwo[0].length * tableThree[0].length;
        double[][] tableTwoThree = new double[rows][columns];

        int counter = 0;
        for (int k = 0; k < tableThree.length; k++) {
            for (int i = 0; i < tableTwo.length; i++) {
                for (int j = 0; j < tableTwo[0].length; j++) {
                    if (String.valueOf(tableTwo[i][j]) != null)
                        tableTwoThree[counter][j] = tableTwo[i][j];
                }
                for (int j = 0; j < tableThree[0].length; j++) {
                    if (String.valueOf(tableThree[k][j]) != null)
                        tableTwoThree[counter][j + 2] = tableThree[k][j];
                }
                counter = counter + 1;
            }
        }
        return tableTwoThree;
    }

    private LinkedHashMap<Double, Double> multiplyTableOneTwoThree(double[][] tableOne, double[][] tableTwoThree) {
        int rows = tableOne.length;
        LinkedHashMap<Double, Double> tableAMap = new LinkedHashMap<Double, Double>();

        for (int i = 0; i < rows; i++) {
            double counter = 0;
            for (int j = 0; j < tableTwoThree.length; j++) {
                if (String.valueOf(tableOne[i][0]) != null && String.valueOf(tableTwoThree[j][0]) != null
                        && String.valueOf(tableTwoThree[j][2]) != null) {
                    if (tableOne[i][0] < (tableTwoThree[j][0] + tableTwoThree[j][2])) {
                        counter = counter + (tableOne[i][1] * tableTwoThree[j][1] * tableTwoThree[j][3]);
                    }
                }
            }
            if (tableAMap.containsKey(tableOne[i][0])) {
                tableAMap.put(tableOne[i][0], (tableAMap.get(tableOne[i][0]) + counter));
            } else {
                tableAMap.put(tableOne[i][0], counter);
            }
        }
        return tableAMap;
    }

    private LinkedHashMap<Double, Double> sortMapFunction(LinkedHashMap<Double, Double> tableAMap) {
        tableAMap = tableAMap.entrySet().stream().sorted((i1, i2) -> i2.getValue().compareTo(i1.getValue())).limit(10)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e2, e1) -> e1, LinkedHashMap::new));
        return tableAMap;
    }

    private void writeExpectedResult(LinkedHashMap<Double, Double> expectedResultMap, Path output,
                                     double[][] tableOne) throws IOException {

        int decimals = checkNumberOfDecimals(tableOne[0][0]);
        try (final BufferedWriter stream = new BufferedWriter(new FileWriter(output.toFile()))) {
            stream.write(Integer.toString(expectedResultMap.size()));
            stream.newLine();

            Iterator it = expectedResultMap.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                double mapKey = Double.parseDouble(pair.getKey().toString());
                double mapValue = Double.parseDouble(pair.getValue().toString());
                stream.write(String.format("%." + decimals + "f", mapKey));
                stream.write(' ');
                stream.write(String.format("%." + decimals + "f", mapValue));
                stream.newLine();
            }

        }
    }

    private int checkNumberOfDecimals(double tableOne) {
        int numberOfDecimals = 0;
        if (String.valueOf(tableOne) != null) {
            if (String.valueOf(tableOne).contains(".")) {
                String decimals = String.valueOf(tableOne);
                if (decimals.split("\\.").length > 1)
                    numberOfDecimals = decimals.split("\\.")[1].length();
                if (numberOfDecimals == 1)
                    numberOfDecimals = 0;
                else if (numberOfDecimals > 1)
                    numberOfDecimals = 6;
            }
        }
        return numberOfDecimals;
    }
}

// Vishal Implementation Ends 29-Nov-21