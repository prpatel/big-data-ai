package dev.prpatel.iceberg.app;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;

import java.time.LocalDateTime;

import static dev.prpatel.iceberg.app.Utilities.formatDataSet;
import static dev.prpatel.iceberg.app.Utilities.formatForHtml;

@Controller
public class HomeController {

    private final AiService aiService;
    @Autowired
    private SparkSession spark;

    @Autowired
    public HomeController(AiService aiService) {
        this.aiService = aiService;
    }

    @GetMapping("/")
    public String home(Model model) {
        return "index";
    }

    @PostMapping("/generatequery")
    public String generatequery(String q, Model model) {
        String generatedQuery = "show me all the properties sold in Clapham";
        System.out.println("question by user:" + q);
        generatedQuery = aiService.generateQuery(q);
        model.addAttribute("result", generatedQuery);
        return "generatequeryresult :: result";
    }

    @PostMapping("/runquery")
    public String runquery(String generatedsql, Model model) {

        String output = " no result ";
        try {
            System.out.println("running query: " + generatedsql);
            Dataset<Row> resultsDF = spark.sql(generatedsql);
            System.out.println("--- Query Results ---");
            long count = resultsDF.count();
            System.out.println("Count:" + count);
            resultsDF.show();
            output = formatForHtml(formatDataSet(resultsDF, 10));
        } catch (Exception e) {
            System.err.println("An error occurred while reading the Iceberg table.");
            e.printStackTrace();
        } finally {

        }
        model.addAttribute("result", output);
        return "runqueryresult :: result";
    }

    @PostMapping("/clicked")
    public String clicked(Model model) {
        model.addAttribute("now", LocalDateTime.now().toString());
        return "clicked :: result";
    }
}