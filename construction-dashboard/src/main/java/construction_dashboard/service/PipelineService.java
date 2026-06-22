package construction_dashboard.service;

import org.springframework.stereotype.Service;

import java.io.File;
import java.nio.charset.StandardCharsets;

@Service
public class PipelineService {

    private static final String COMPOSE_DIR =
            "../kafka";

    public String runDbtPipeline() throws Exception {
        ProcessBuilder pb = new ProcessBuilder(
                "docker",
                "compose",
                "run",
                "--rm",
                "dbt-pipeline"
        );

        pb.directory(new File(COMPOSE_DIR));
        pb.redirectErrorStream(true);

        Process process = pb.start();

        String output = new String(
                process.getInputStream().readAllBytes(),
                StandardCharsets.UTF_8
        );

        int exitCode = process.waitFor();

        return "Exit code: " + exitCode + "\n\n" + output;
    }
}