package ch.uzh.ddis.thesis.lambda_architecture.dataset_statistics;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
public class Calculator {
    private static final Logger logger = LogManager.getLogger();

    @Parameter(names = "-dataPath", description = "path to dataset", required = true)
    public String dataPath;

    public void start() throws IOException{
        SummaryStatistics stats = new SummaryStatistics();

        BufferedReader reader = new BufferedReader(new FileReader(dataPath));
        try {
            String line;
            while ((line = reader.readLine()) != null) {
                long size = 8 * (int) ((((line.length()) * 2) + 45) / 8);

                stats.addValue(size);
            }
        } catch (IOException e){
            logger.error("could not read csv line with error message `{}`", e.getMessage());
        }

        try {
            reader.close();
        } catch (IOException e) {
            logger.error(e);
        }

        System.out.println("dataset: " + new File(dataPath).getName());
        System.out.println("# of records: " + stats.getN());
        System.out.println("avg: " + stats.getMean() + " bytes");
        System.out.println("max: " + stats.getMax() + " bytes");
        System.out.println("min: " + stats.getMin() + " bytes");
        System.out.println("variance: " + stats.getVariance());
        System.out.println("std deviation: " + stats.getStandardDeviation());
    }



    public static void main(String[] args) {
        Calculator calculator = new Calculator();
        JCommander j = new JCommander(calculator, args);

        if(calculator.dataPath == null){
            j.usage();
            System.exit(1);
        }

        try {

            calculator.start();

            System.exit(0);
        } catch (IOException e){
            System.out.println("Something went terribly wrong! error = `" + e + "`");
        }
    }
}
