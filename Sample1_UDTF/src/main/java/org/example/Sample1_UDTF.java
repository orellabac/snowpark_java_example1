package org.example;


import org.apache.commons.math3.optim.PointValuePair;
import org.apache.commons.math3.optim.linear.*;

import org.apache.commons.math3.optim.nonlinear.scalar.GoalType;

import java.util.*;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.snowflake.snowpark_java.Row;



class Result {
    public String day;
    public double optimal_A;
    public double optimal_B;
    public double maxProfit;
    public Result(String day,double optimalA, double optimalB, Double maxProfit) {
        this.day = day;
        this.optimal_A = optimalA;
        this.optimal_B = optimalB;
        this.maxProfit = maxProfit.doubleValue();
    }
        
}

public class Sample1_UDTF {
    private static final Logger logger = LoggerFactory.getLogger(Sample1_UDTF.class);

    public static void main(String[] args) {
        var session = SFUtils.getSession();
        var input_data = session.table(args[0]);
        var udtf = new Sample1_UDTF();
        Row[] records = input_data.collect();
        for (var record : records) {
            var results = udtf.process(record.getString(0), record.getFloat(1), record.getFloat(2)).collect(Collectors.toList());
            var result = results.get(0);
            System.out.println(result.day + "," + result.optimal_A + "," + result.optimal_B + "," + result.maxProfit);
        }
    }

    @SuppressWarnings("rawtypes")
    public static Class getOutputClass() {
        return Result.class;
    }

    public java.util.stream.Stream<Result> process(String day, Float availableMaterial, Float availableLabor) {
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
            logger.trace("Product A: " + result.optimal_A);
            logger.trace("Product B: " + result.optimal_B);
            logger.trace("Maximum Profit: " + result.maxProfit);
            return java.util.stream.Stream.of(result);

    }

}
