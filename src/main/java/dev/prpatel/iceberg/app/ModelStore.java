package dev.prpatel.iceberg.app;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * Which model answers the next question.
 *
 * Same two layers as {@link PromptStore}: the default comes from
 * {@code spring.ai.openai.chat.model}, and anything chosen on the admin page is written to
 * {@code data/config/model.txt} and wins from the next request onwards - no restart, no redeploy.
 * On Spaces that path is on the mounted volume, so a choice survives a restart.
 *
 * The point of doing this at request time rather than at startup is that comparing models stops
 * costing a deploy each: pick, ask, pick again.
 */
@Component
public class ModelStore {

    static final Path OVERRIDE = Paths.get("data", "config", "model.txt");

    /**
     * The models offered in the dropdown. Each is {@code <model-id>:<provider>} as understood by
     * the Inference Providers router - the suffix pins one provider, rather than letting the
     * router pick. {@code :fastest} and {@code :cheapest} are policies you can type into Other.
     */
    public static final List<String> CATALOGUE = List.of(
            "Qwen/Qwen3.6-27B:ovhcloud",
            "Qwen/Qwen3.6-35B-A3B:scaleway",
            "Qwen/Qwen3-Coder-Next:novita",
            "openai/gpt-oss-120b:groq",
            "google/gemma-3-27b-it:deepinfra",
            "google/gemma-3-1b-it:featherless-ai"
    );

    private final String configuredDefault;

    public ModelStore(@Value("${spring.ai.openai.chat.model}") String configuredDefault) {
        this.configuredDefault = configuredDefault;
    }

    /** The model to use right now. */
    public String get() {
        String override = readOverride();
        return override != null ? override : configuredDefault;
    }

    /** What application.properties asks for, ignoring any override. */
    public String getDefault() {
        return configuredDefault;
    }

    public boolean isCustomised() {
        return readOverride() != null;
    }

    /** Every option the dropdown should show: the configured default first, then the catalogue. */
    public List<String> options() {
        return java.util.stream.Stream.concat(
                        java.util.stream.Stream.of(configuredDefault), CATALOGUE.stream())
                .distinct()
                .toList();
    }

    /** Persist a choice. Blank is treated as "go back to the configured default". */
    public void save(String model) {
        if (model == null || model.isBlank()) {
            reset();
            return;
        }
        try {
            Files.createDirectories(OVERRIDE.getParent());
            Files.writeString(OVERRIDE, model.strip() + System.lineSeparator(), StandardCharsets.UTF_8);
            System.out.println("✅ Model set to " + model.strip());
        } catch (IOException e) {
            throw new UncheckedIOException("Could not save the model choice", e);
        }
    }

    public void reset() {
        try {
            if (Files.deleteIfExists(OVERRIDE)) {
                System.out.println("✅ Model reset to the configured default (" + configuredDefault + ")");
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Could not reset the model choice", e);
        }
    }

    private String readOverride() {
        try {
            if (!Files.isRegularFile(OVERRIDE)) {
                return null;
            }
            String text = Files.readString(OVERRIDE, StandardCharsets.UTF_8).strip();
            return text.isBlank() ? null : text;
        } catch (IOException e) {
            System.err.println("Could not read " + OVERRIDE + ", using the configured model: " + e.getMessage());
            return null;
        }
    }
}
