package dev.prpatel.iceberg.app;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.time.LocalDateTime;

import static dev.prpatel.iceberg.app.Utilities.formatDataSet;
import static dev.prpatel.iceberg.app.Utilities.formatForHtml;

@Controller
public class HomeController {

    private final AiService aiService;
    private final SqlGuard sqlGuard;
    @Autowired
    private SparkSession spark;

    @Autowired
    public HomeController(AiService aiService, SqlGuard sqlGuard) {
        this.aiService = aiService;
        this.sqlGuard = sqlGuard;
    }

    @GetMapping("/")
    public String home(Model model) {
        return "index";
    }

    @PostMapping("/generatequery")
    public String generatequery(String q, Model model) {
        System.out.println("question by user:" + q);
        try {
            SqlAnswer answer = aiService.generateAnswer(q);
            model.addAttribute("result", answer.sql());
            model.addAttribute("assumptions", answer.assumptions());
            model.addAttribute("columnsUsed", String.join(", ", answer.columnsUsed()));
        } catch (AiService.UnstructuredResponseException e) {
            // Shown rather than repaired. The contract lives in the system prompt, so this is a
            // prompt to fix on /admin - stripping fences here would only hide it.
            model.addAttribute("result", "");
            model.addAttribute("error", e.getMessage()
                    + " Edit the system prompt on /admin. What came back was:");
            model.addAttribute("raw", e.getRaw());
        }
        return "generatequeryresult :: result";
    }

    @PostMapping("/runquery")
    public String runquery(String generatedsql,
                           @RequestParam(name = "maxrows", required = false, defaultValue = "0") int maxrows,
                           Model model) {

        String output;
        try {
            // Never spark.sql() straight from the page. This box is an ordinary form field, so its
            // contents are whatever the browser sent - the model is not the only thing that can put
            // a DROP TABLE in it.
            SqlGuard.CheckedSql checked = sqlGuard.check(generatedsql, maxrows);
            System.out.println("running query: " + checked.sql());

            Dataset<Row> resultsDF = spark.sql(checked.sql());
            output = formatForHtml(formatDataSet(resultsDF, sqlGuard.maxRows()));
            if (checked.limitApplied()) {
                output = output + "\n\n(row cap applied)";
            }
        } catch (SqlGuard.RejectedException e) {
            System.out.println("rejected: " + e.getMessage());
            output = "Rejected: " + e.getMessage();
        } catch (Exception e) {
            System.err.println("An error occurred while reading the Iceberg table.");
            e.printStackTrace();
            output = "Query failed: " + e.getMessage();
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