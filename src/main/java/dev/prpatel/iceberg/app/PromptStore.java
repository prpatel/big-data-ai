package dev.prpatel.iceberg.app;

import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Holds the text-to-SQL system prompt outside the code, so it can be edited without a rebuild.
 *
 * There are two layers. The default ships in the jar at {@code prompts/text-to-sql-system.txt} and
 * is what a fresh deployment uses. Anything saved through the admin page is written to
 * {@code data/prompts/text-to-sql-system.txt} and wins from the next request onwards - no restart,
 * no redeploy. That path is relative to the working directory, the same convention
 * {@link dev.prpatel.iceberg.tools.IcebergService} uses for downloaded CSVs, so on Spaces it lands
 * on the mounted volume and survives a restart.
 *
 * The override is read on every call. The file is a couple of kilobytes and a question already
 * costs an LLM round trip, so caching it would buy nothing and would only add a staleness bug.
 */
@Component
public class PromptStore {

    static final String CLASSPATH_DEFAULT = "prompts/text-to-sql-system.txt";
    static final Path OVERRIDE = Paths.get("data", "prompts", "text-to-sql-system.txt");

    /** The prompt to use right now: the saved override if there is one, otherwise the default. */
    public String get() {
        String override = readOverride();
        return override != null ? override : getDefault();
    }

    /** The prompt as shipped, ignoring any override. */
    public String getDefault() {
        try (var in = new ClassPathResource(CLASSPATH_DEFAULT).getInputStream()) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("Default system prompt is missing from the jar", e);
        }
    }

    /** True when an edited prompt is in force, so the UI can say so. */
    public boolean isCustomised() {
        return readOverride() != null;
    }

    /** Persist an edited prompt. Blank input is treated as a reset rather than an empty prompt. */
    public void save(String prompt) {
        if (prompt == null || prompt.isBlank()) {
            reset();
            return;
        }
        try {
            Files.createDirectories(OVERRIDE.getParent());
            Files.writeString(OVERRIDE, prompt.strip() + System.lineSeparator(), StandardCharsets.UTF_8);
            System.out.println("✅ System prompt saved to " + OVERRIDE.toAbsolutePath());
        } catch (IOException e) {
            throw new UncheckedIOException("Could not save the system prompt", e);
        }
    }

    /** Drop the override and go back to the prompt that ships in the jar. */
    public void reset() {
        try {
            if (Files.deleteIfExists(OVERRIDE)) {
                System.out.println("✅ System prompt reset to the packaged default");
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Could not reset the system prompt", e);
        }
    }

    private String readOverride() {
        try {
            if (!Files.isRegularFile(OVERRIDE)) {
                return null;
            }
            String text = Files.readString(OVERRIDE, StandardCharsets.UTF_8);
            return text.isBlank() ? null : text;
        } catch (IOException e) {
            // A broken override should not take the app down - fall back and say why.
            System.err.println("Could not read " + OVERRIDE + ", using the packaged prompt: " + e.getMessage());
            return null;
        }
    }
}
