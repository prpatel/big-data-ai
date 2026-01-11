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

    @Autowired
    public AdminController(IcebergService icebergService) {
        this.icebergService = icebergService;
    }

    @GetMapping
    public String index(Model model) {
        List<String> files = icebergService.listDownloadedFiles();
        model.addAttribute("files", files);
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
