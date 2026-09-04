package dev.prpatel.iceberg.app;

import dev.prpatel.iceberg.tools.IcebergService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;

@Controller
@RequestMapping("/admin")
class AdminController {

    private final IcebergService icebergService;
    private final PromptStore promptStore;
    private final ModelStore modelStore;
    private final ParquetExporter exporter;

    @Autowired
    public AdminController(IcebergService icebergService, PromptStore promptStore,
                           ModelStore modelStore, ParquetExporter exporter) {
        this.icebergService = icebergService;
        this.promptStore = promptStore;
        this.modelStore = modelStore;
        this.exporter = exporter;
    }

    @GetMapping
    public String index(Model model) {
        List<String> files = icebergService.listDownloadedFiles();
        model.addAttribute("files", files);
        model.addAttribute("systemPrompt", promptStore.get());
        model.addAttribute("promptCustomised", promptStore.isCustomised());
        model.addAttribute("modelOptions", modelStore.options());
        model.addAttribute("activeModel", modelStore.get());
        model.addAttribute("defaultModel", modelStore.getDefault());
        model.addAttribute("modelCustomised", modelStore.isCustomised());
        model.addAttribute("exportFiles", exporter.listExport("uk-price-paid"));
        return "admin";
    }

    @PostMapping("/setup")
    public String setup(Model model) {
        icebergService.setup();
        model.addAttribute("message", "Setup operation initiated.");
        return "admin_result :: result";
    }

    @PostMapping("/clear")
    public String clear(Model model) {
        icebergService.clear();
        model.addAttribute("message", "Clear operation initiated.");
        return "admin_result :: result";
    }

    @PostMapping("/export")
    public String export(@RequestParam(name = "files", required = false, defaultValue = "8") int files,
                         Model view) {
        try {
            ParquetExporter.Result r = exporter.export(null, "uk-price-paid", files);
            view.addAttribute("message", String.format(
                    "Exported %,d rows to %d parquet files (%s) in %s. Reload to see them, then publish with the Job below.",
                    r.rows(), r.files(), r.humanBytes(), r.path()));
        } catch (Exception e) {
            view.addAttribute("message", "Export failed: " + e.getMessage()
                    + " (is the table loaded? try Load Data first)");
        }
        return "admin_result :: result";
    }

    @PostMapping("/model")
    public String saveModel(@RequestParam(name = "model", required = false) String model,
                            @RequestParam(name = "otherModel", required = false) String otherModel,
                            Model view) {
        // "OTHER" is the escape hatch in the dropdown; the real value is in the text box next to it.
        String chosen = "OTHER".equals(model) ? otherModel : model;
        if ("OTHER".equals(model) && (otherModel == null || otherModel.isBlank())) {
            view.addAttribute("message", "Pick OTHER and paste a model id, e.g. openai/gpt-oss-120b:cheapest");
            return "admin_result :: result";
        }
        modelStore.save(chosen);
        view.addAttribute("message", "Now asking " + modelStore.get()
                + ". Applies to the next question - no restart needed.");
        return "admin_result :: result";
    }

    @PostMapping("/model/reset")
    public String resetModel(Model view) {
        modelStore.reset();
        view.addAttribute("message", "Model reset to " + modelStore.getDefault() + ". Reload this page to see it.");
        return "admin_result :: result";
    }

    @PostMapping("/prompt")
    public String savePrompt(@RequestParam(name = "prompt", required = false) String prompt, Model model) {
        promptStore.save(prompt);
        model.addAttribute("message", promptStore.isCustomised()
                ? "System prompt saved. It applies to the next question - no restart needed."
                : "Prompt was empty, so the packaged default is back in force.");
        return "admin_result :: result";
    }

    @PostMapping("/prompt/reset")
    public String resetPrompt(Model model) {
        promptStore.reset();
        model.addAttribute("message", "System prompt reset to the packaged default. Reload this page to see it.");
        return "admin_result :: result";
    }

    @PostMapping("/download")
    public String download(@RequestParam(name = "year", required = false) String year, Model model) {
        icebergService.download(year);
        model.addAttribute("message", "Download operation initiated for year: " + year);
        return "admin_result :: result";
    }

    @PostMapping("/load")
    public String load(@RequestParam(name = "year", required = false) String year, Model model) {
        icebergService.load(year);
        String message = (year != null && !year.isEmpty()) 
            ? "Load operation initiated for year: " + year 
            : "Load operation initiated for all files.";
        model.addAttribute("message", message);
        return "admin_result :: result";
    }
}
