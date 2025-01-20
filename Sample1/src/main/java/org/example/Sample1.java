package org.example;


import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.math3.optim.PointValuePair;
import org.apache.commons.math3.optim.linear.*;

import org.apache.commons.math3.optim.nonlinear.scalar.GoalType;

import java.io.*;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class Result {
    public String day;
    public double optimalA;
    public double optimalB;
    public double maxProfit;
    public Result(String day,double optimalA, double optimalB, Double maxProfit) {
        this.day = day;
        this.optimalA = optimalA;
        this.optimalB = optimalB;
        this.maxProfit = maxProfit.doubleValue();
    }
        
}

public class Sample1 {
    private static final Logger logger = LoggerFactory.getLogger(Sample1.class);

    public static void main(String[] args) {
        runProcess(args);
    }

    public static String runProcess(String[] args) {
        String lastMessage;
        String input_data = args[0];
        try {
            // Step 1: Extract
            logger.info("Extracting data...");
            InputStream inputStream;
            inputStream = new FileInputStream(input_data);
            Reader in = new InputStreamReader(inputStream);
            Iterable<CSVRecord> records = CSVFormat.DEFAULT.builder().setHeader().build().parse(in);
            int availableMaterial = 0;
            int availableLabor = 0;
            String day;
            var results = new ArrayList<Result>();
            for (var record : records) {
                day = record.get("Day");
                availableMaterial = Integer.parseInt(record.get("Material"));
                availableLabor = Integer.parseInt(record.get("Labor"));

                logger.info("Available Material: " + availableMaterial);
                logger.info("Available Labor: " + availableLabor);
    
                // Step 2: Transform (Linear Programming)
                logger.info("Performing optimization...");
    
                // Define the objective function (maximize 50A + 40B)
                double[] objectiveCoefficients = {50, 40};
    
                // Define constraints
                Collection<LinearConstraint> constraints = new ArrayList<>();
                constraints.add(new LinearConstraint(new double[]{4, 2}, Relationship.LEQ, availableMaterial)); // 4A + 2B <= Material
                constraints.add(new LinearConstraint(new double[]{2, 5}, Relationship.LEQ, availableLabor)); // 2A + 5B <= Labor
                constraints.add(new LinearConstraint(new double[]{1, 0}, Relationship.GEQ, 0)); // A >= 0
                constraints.add(new LinearConstraint(new double[]{0, 1}, Relationship.GEQ, 0)); // B >= 0
    
                // Set up and solve
                LinearObjectiveFunction function = new LinearObjectiveFunction(objectiveCoefficients, 0);
                SimplexSolver solver = new SimplexSolver();
                PointValuePair solution = solver.optimize(
                    new LinearConstraintSet(constraints),
                    function,
                    GoalType.MAXIMIZE,
                    new NonNegativeConstraint(true)
                );
                var result = new Result(day, solution.getPoint()[0], solution.getPoint()[1], solution.getValue());
    
                logger.trace("Optimal Production:");
                logger.trace("Product A: " + result.optimalA);
                logger.trace("Product B: " + result.optimalB);
                logger.trace("Maximum Profit: " + result.maxProfit);

                results.add(result);

            }
            in.close();
            
            // Step 3: Load
            String output_data = args[1];
            logger.info("Loading optimized results...");
            
            FileWriter writer = new FileWriter(output_data);
            CSVPrinter printer = new CSVPrinter(writer, CSVFormat.DEFAULT.builder().setHeader("DAY","OPTIMAL_A", "OPTIONAL_B","MAXPROFIT").build());
            for(var result : results) {
                printer.printRecord(result.day, result.optimalA, result.optimalB, result.maxProfit);
            }
            printer.close();
            writer.close();
            
            lastMessage = "ETL process complete. Results saved to: " + output_data;
            logger.info(lastMessage);
            return lastMessage;
        } catch (Exception e) {
            lastMessage = "Error occurred during ETL process";
            logger.error(lastMessage, e);
            return lastMessage;
        }
    }
}
