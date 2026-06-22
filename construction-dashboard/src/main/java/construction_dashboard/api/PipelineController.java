package construction_dashboard.api;

import construction_dashboard.service.PipelineService;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/pipeline")
public class PipelineController {

    private final PipelineService pipelineService;

    public PipelineController(PipelineService pipelineService) {
        this.pipelineService = pipelineService;
    }

    @PostMapping("/run")
    public String runPipeline() throws Exception {
        return pipelineService.runDbtPipeline();
    }
}