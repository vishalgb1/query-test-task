package org.query.calc;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.file.Path;
import java.util.concurrent.Callable;

public class TableReader implements Callable<double[][]> {
    String separator;
    Path filePath;

    public TableReader(String separator, Path filePath) {
        super();
        this.separator = separator;
        this.filePath = filePath;
    }

    @Override
    public double[][] call() throws Exception {
        // TODO Auto-generated method stub
        BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath.toFile()));
        String numberOfRows = bufferedReader.readLine();
        int tableRows = 0;
        double[][] table = null;
        if (numberOfRows != null && numberOfRows.length() > 0)
            tableRows = Integer.parseInt(numberOfRows);
        if (tableRows != 0 && tableRows > 0)
            table = new double[tableRows][2];

        for (int i = 0; i < tableRows; i++) {
            String matrixRows = bufferedReader.readLine();
            if (matrixRows.split(separator, 0).length > 0) {
                table[i][0] = Double.parseDouble(matrixRows.split(separator, 0)[0]);

            }
            if (matrixRows.split(separator, 0).length > 1)
                table[i][1] = Double.parseDouble(matrixRows.split(separator, 0)[1]);
        }
        bufferedReader.close();
        return table;
    }

}